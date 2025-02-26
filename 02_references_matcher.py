import logging
import os
import time
import mysql.connector
import json
import datetime

##建立日志

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# 用当前时间生成一个字符串(yyyyMMdd_HHMMSS)
timestamp = time.strftime("%Y%m%d_%H%M%S")

log_filename = os.path.join(SCRIPT_DIR, f'references_matcher_{timestamp}.log')

logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,  # 或者 DEBUG/ERROR 等
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'  # 每次运行新建文件，而不是追加
)


##开始匹配
DB_CONFIG = {
    'host': '147.8.219.19',
    'port': 3306,
    'database': 'my_crawler_db',
    'user': 'crawler_gexinlin',
    'password': 'QAZwsx520'
}

def text_similarity(a, b):
    """
    一个非常简易的文本相似度函数，返回 0~1 之间的相似度。
    可以使用更先进的方法：如 difflib.SequenceMatcher、nltk edit distance 等。
    """
    if not a or not b:
        return 0.0
    # 这里演示最简洁的：基于公共字符数 / 平均长度
    a_set = set(a.lower().split())
    b_set = set(b.lower().split())
    common = len(a_set.intersection(b_set))
    avg_len = (len(a_set) + len(b_set)) / 2.0
    if avg_len == 0:
        return 0.0
    return common / avg_len

def find_referenced_posts(refs, conn, referencing_post):
    """
    根据 'refs' 中的引用信息，在同 topic_id 下、post_time 更早的帖子中，
    找到作者相同且与 'quoted_content' 最相似的一条帖子。
    
    referencing_post: 一个包含 { 'post_id', 'topic_id', 'post_time' ... }
    """

    referencing_post_id = referencing_post['post_id']
    referencing_topic_id = referencing_post['topic_id']
    referencing_time = referencing_post['post_time']

    logging.info(f"=== Analyzing referencing_post_id={referencing_post_id} (topic_id={referencing_topic_id}, time={referencing_time}) ===")

    matched_ids = []
    cur = conn.cursor()

    # 遍历每个引用对象
    for idx, ref_item in enumerate(refs, start=1):
        quoted_author = ref_item.get("quoted_author", "").strip()
        quoted_content = ref_item.get("quoted_content", "").strip()

        logging.info(f"  [Ref#{idx}] quoted_author={quoted_author!r}, content_snippet={quoted_content[:50]!r}...")

        # 如果缺少作者或引用正文，跳过
        if not quoted_author or not quoted_content:
            logging.info("    => Skipped: quoted_author or quoted_content is empty.")
            continue

        # 1) 找到同 topic_id、同 author、发帖时间早于 referencing_time 的帖子
        cur.execute("""
            SELECT post_id, content, post_time
              FROM posts
             WHERE topic_id = %s
               AND author = %s
               AND post_time < %s
        """, (referencing_topic_id, quoted_author, referencing_time))
        candidates = cur.fetchall()  # [(post_id, content, post_time), ...]

        logging.info(f"    => Found {len(candidates)} candidate(s) in topic {referencing_topic_id}, author={quoted_author}, post_time < {referencing_time}.")

        if not candidates:
            logging.info("    => No candidates. Skip.")
            continue

        # 在这些 candidates 里，找与 quoted_content 最相似的一条
        best_match_id = None
        best_similarity = 0.0

        for (c_post_id, c_content, c_time) in candidates:
            sim = text_similarity(quoted_content, c_content)
            # 打印一下相似度
            logging.info(f"      candidate post_id={c_post_id}, post_time={c_time}, sim={sim:.3f}")

            if sim > best_similarity:
                best_similarity = sim
                best_match_id = c_post_id

        # 设一个相似度阈值，太低就认为“没匹配”
        similarity_threshold = 0.2  # 可自行调整
        if best_match_id and best_similarity >= similarity_threshold:
            logging.info(f"    => Best match: post_id={best_match_id}, similarity={best_similarity:.3f} (>= {similarity_threshold})")
            matched_ids.append(best_match_id)
        else:
            logging.info(f"    => No match or sim too low (best_similarity={best_similarity:.3f}, threshold={similarity_threshold})")

    cur.close()
    logging.info(f"=== Done analyzing referencing_post_id={referencing_post_id} ===\n")
    return matched_ids

def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    # 1) 取出所有帖子（至少需要 post_time, topic_id, post_id, post_references）
    cur.execute("""
        SELECT post_id, topic_id, author, content, post_time, post_references
          FROM posts
    """)
    rows = cur.fetchall()

    # 为了便于后面函数使用，先把数据放成字典
    posts_data = []
    for row in rows:
        post_id, topic_id, author, content, post_time, post_refs = row
        post_dict = {
            'post_id': post_id,
            'topic_id': topic_id,
            'author': author,
            'content': content or "",
            'post_time': post_time,  # datetime
            'post_references': post_refs
        }
        posts_data.append(post_dict)

    logging.info(f"共有 {len(posts_data)} 条帖子需要处理。\n")

    # 2) 遍历每条帖子，对其 post_references 做匹配
    inserted_count = 0
    for p in posts_data:
        pid = p['post_id']
        refs_json = p['post_references']

        # 若 post_references 为空就跳过
        if not refs_json:
            continue
        
        # 新：先判断类型
        if isinstance(refs_json, str):
            # 数据库里存的还是字符串
            try:
                refs = json.loads(refs_json)
            except json.JSONDecodeError:
                refs = []
        elif isinstance(refs_json, list) or isinstance(refs_json, dict):
            # psycopg2 已经转换好了，直接用
            refs = refs_json
        else:
            refs = []

        # 如果是空列表，也跳过
        if not refs:
            continue

        matched_ids = find_referenced_posts(refs, conn, p)
        # matched_ids 可能包含多个被引用 post_id

        for ref_id in matched_ids:
            try:
                # MySQL 用 INSERT IGNORE 来实现“不重复插入”
                cur.execute("""
                    INSERT IGNORE INTO post_references (referencing_post_id, referenced_post_id)
                    VALUES (%s, %s)
                """, (pid, ref_id))
                if cur.rowcount > 0:
                    inserted_count += 1
            except Exception as e:
                logging.info("插入引用关系失败:", e)

    conn.commit()
    cur.close()
    conn.close()

    logging.info(f"离线引用匹配完成。总共插入了 {inserted_count} 条引用关系。")

if __name__ == "__main__":
    main()
