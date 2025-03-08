import mysql.connector
import json
import logging
import datetime
import re
import os
import time

# 1) 生成时间戳字符串，例如 "20250305_173245"
timestamp = time.strftime("%Y%m%d_%H%M%S")

# 2) 拼出日志文件名，比如 "topic_tree_20250305_173245.log"
log_filename = f"topic_tree_{timestamp}.log"

# 3) 如果你想把日志放到脚本同级目录，就直接用这个 log_filename
#    若想放子目录，也可用 os.path.join(...)
#    log_filename = os.path.join("logs", f"topic_tree_{timestamp}.log")

logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,   # 或 DEBUG
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logging.info("日志系统初始化完毕。")


# ========== 多层解析所需的正则 & 函数 ==========

quote_start_pattern = re.compile(r'\[quote(?:=([^]]+))?\]', re.IGNORECASE)
quote_end_pattern = re.compile(r'\[/quote\]', re.IGNORECASE)

def parse_forum_quotes(post_text):
    """
    解析包含 [quote=xxx]...[/quote]（可多层嵌套）的帖子内容。
    返回一棵嵌套结构:
    {
      "content": "最外层文本(已去除quote标签)",
      "children": [
         {
           "author": "xxx",
           "content": "该quote层去掉子quote后剩余文本",
           "children": [ ... ]
         },
         ...
      ]
    }
    """
    root = {"content": "", "children": []}
    stack = [root]
    idx = 0
    while True:
        start_match = quote_start_pattern.search(post_text, idx)
        end_match = quote_end_pattern.search(post_text, idx)
        if not start_match and not end_match:
            stack[-1]["content"] += post_text[idx:]
            break
        if start_match and end_match:
            next_pos = min(start_match.start(), end_match.start())
        else:
            next_pos = start_match.start() if start_match else end_match.start()
        # 先把标签前面的文本放进当前节点
        stack[-1]["content"] += post_text[idx:next_pos]
        if start_match and start_match.start() == next_pos:
            # [quote=xxx]
            author = start_match.group(1)
            if author is not None:
                author = author.strip()
            idx = start_match.end()
            new_node = {"author": author, "content": "", "children": []}
            stack[-1]["children"].append(new_node)
            stack.append(new_node)
        else:
            # [/quote]
            idx = end_match.end()
            if len(stack) > 1:
                stack.pop()
    return root

def collect_refs_and_cleaned_text(root_node):
    """
    DFS 遍历 parse_forum_quotes 结果, 收集多层引用:
    返回 (refs, cleaned_text)
      refs: [ {"quoted_author":..., "quoted_content":...}, ... ]
    """
    refs = []
    def dfs(node):
        if node.get("author") is not None:
            # 这是一个引用层
            refs.append({
                "quoted_author": node["author"],
                "quoted_content": node["content"].strip()
            })
        for c in node["children"]:
            dfs(c)
    dfs(root_node)

    # 拼接 cleaned_text
    def gather_text(node):
        txt = node["content"]
        for c in node["children"]:
            txt += gather_text(c)
        return txt
    cleaned_text = gather_text(root_node).strip()

    return refs, cleaned_text
  
 # ========== 数据库配置 ==========

DB_CONFIG = {
    'host': '147.8.219.19',
    'port': 3306,
    'database': 'my_crawler_db',
    'user': 'crawler_gexinlin',
    'password': 'QAZwsx520'
}

# ========== 引用匹配函数 ==========

def match_quoted_post(topic_id, quoted_author, quoted_content, posts_list):
    """
    尝试在 posts_list 中找出 被引用的那个 post_number。
    简化逻辑：先找 'author == quoted_author' 的帖子,
              再看 'quoted_content' 是否是其 post_canonical 的子串.
              若找到唯一匹配, 返回其 post_number. 否则返回 0 表示无法确定.
    """
    # 先筛选出 author 相符的
    candidates = [p for p in posts_list if p["author"] == quoted_author]
    if not candidates:
        logging.debug(f"match_quoted_post: author '{quoted_author}' 无匹配 -> lost=0")
        return 0
      
    best_match_number = 0
    for c in candidates:
      # 如果quoted_content是c["post_canonical"]的子串
        if quoted_content and (quoted_content in c["post_canonical"]):
            logging.debug(f"match_quoted_post: author='{quoted_author}', content匹配 -> post_number={c['post_number']}")
            return c["post_number"]

    logging.debug(f"match_quoted_post: author='{quoted_author}' 找到候选但内容不匹配 -> lost=0")
    return 0


  
# ========== 核心: 构建 tree_json ==========
def build_tree_for_topic(topic_id):
    """
    从 posts 表里读取 topic_id 对应的所有帖子,
    多层解析 [quote=xxx] -> (quoted_author, quoted_content),
    并结合 match_quoted_post 去找被引用的帖子 => 构建 edges.
    """
    logging.info(f"开始构建树状结构: topic_id={topic_id}")
    
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor(dictionary=True)

    # 1) 拿到 topic_title
    cur.execute("SELECT topic_title FROM topics WHERE topic_id = %s", (topic_id,))
    row_topic = cur.fetchone()
    topic_title = row_topic["topic_title"] if row_topic else ""
    logging.info(f"话题标题: {topic_title or '[未知]'}")

    # 2) 获取所有 posts
    cur.execute("""
        SELECT post_id, author, post_time, post_canonical
        FROM posts
        WHERE topic_id = %s
        ORDER BY post_time ASC
    """, (topic_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        logging.warning(f"topic_id={topic_id} 下没有任何帖子.")
        return {
            "topic_title": topic_title,
            "posts_num": 0,
            "posts_get": 0,
            "tree_json": json.dumps({"nodes": [], "edges": []}, ensure_ascii=False)
        }
    logging.info(f"topic_id={topic_id} 下共{len(rows)}条帖子.")

    # 给帖子分配本地序号 n=1..N
    posts_list = []
    for i, r in enumerate(rows):
        post_number = i+1
        post_time_str = ""
        if r["post_time"]:
            post_time_str = r["post_time"].strftime("%Y-%m-%d %H:%M:%S")
        posts_list.append({
            "post_number": post_number,
            "post_id": r["post_id"],
            "author": r["author"],
            "post_time": post_time_str,
            "post_canonical": r["post_canonical"] or ""
        })

    edges = []
    N = len(posts_list)

    # 3) 对每个帖解析多层引用 => (quoted_author, quoted_content)
    for post_obj in posts_list:
        n = post_obj["post_number"]
        text = post_obj["post_canonical"]
        logging.debug(f"[topic_id={topic_id}] 处理第{n}帖 post_id={post_obj['post_id']}")


        # a) 多层解析
        root_struct = parse_forum_quotes(text)
        refs, cleaned_text = collect_refs_and_cleaned_text(root_struct)
        logging.debug(f"解析到 {len(refs)} 条引用. (无引用则0)")

        if not refs:
             # b) 无引用 => 如果不是第一楼 => n->n-1
            if n>1:
                edges.append({"from": n, "to": n-1})
                logging.debug(f"无引用, 且非首贴 => edge: {n}->{n-1}")
        else:
            # c) 有引用 => 每条 ref
            for ref in refs:
                quoted_author   = ref["quoted_author"] or ""
                quoted_content  = ref["quoted_content"] or ""
                logging.debug(f"  引用ref => author='{quoted_author}', content长度={len(quoted_content)}")

                # 尝试匹配
                m = match_quoted_post(topic_id, quoted_author, quoted_content, posts_list)
                if m == 0:
                    # lost
                    edges.append({"from": n, "to": 0})
                    logging.debug(f"  match失败 => (n->0) => {n}->0")
                else:
                    edges.append({"from": n, "to": m})
                    logging.debug(f"  match成功 => edge: {n}->{m}")

    # 4) 组装 nodes
    nodes = []
    for p in posts_list:
        nodes.append({
            "post_number": p["post_number"],
            "post_id": p["post_id"],
            "author": p["author"],
            "post_time": p["post_time"]
        })

    tree_data = {
        "nodes": nodes,
        "edges": edges
    }
    tree_json_str = json.dumps(tree_data, ensure_ascii=False)
    logging.info(f"完成构建树状结构: topic_id={topic_id}, 帖子数={N}, edges={len(edges)}")
    
    return {
        "topic_title": topic_title,
        "posts_num": N,
        "posts_get": N,
        "tree_json": tree_json_str
    }

def insert_post_tree(topic_id):
    """
    调用 build_tree_for_topic(topic_id) 构建多层解析结果 => 插表 post_trees
    """
    result = build_tree_for_topic(topic_id)
    topic_title = result["topic_title"]
    posts_num = result["posts_num"]
    posts_get = result["posts_get"]
    tree_json_str = result["tree_json"]

    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    sql = """
    INSERT INTO post_trees (topic_id, topic_title, posts_num, posts_get, tree_json)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      topic_title = VALUES(topic_title),
      posts_num = VALUES(posts_num),
      posts_get = VALUES(posts_get),
      tree_json = VALUES(tree_json)
    """
    cur.execute(sql, (topic_id, topic_title, posts_num, posts_get, tree_json_str))
    conn.commit()
    cur.close()
    conn.close()
    logging.info(f"[topic_id={topic_id}] 已成功生成并插入 tree_json.")

def main():
    test_topic_id = 463183
    insert_post_tree(test_topic_id)

if __name__=="__main__":
    main()