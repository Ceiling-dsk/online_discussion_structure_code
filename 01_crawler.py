import requests
import json
import time
import random
import logging
import datetime
import re
import mysql.connector
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ============ 日志配置 ============

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# 用当前时间生成一个字符串，比如 "20250226_192211"
timestamp = time.strftime("%Y%m%d_%H%M%S")

# 拼出一个唯一的日志文件路径
log_filename = os.path.join(SCRIPT_DIR, f'crawler_{timestamp}.log')

logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'  # 以写模式打开（非追加），这样每次运行都会生成一个新日志文件
)

# ============ 数据库配置(请根据实际情况修改) ============
DB_CONFIG = {
    'host': '147.8.219.19',   # 云端 MySQL 主机/IP
    'port': 3306,          # 默认 MySQL 端口
    'database': 'my_crawler_db',
    'user': 'crawler_gexinlin',
    'password': 'QAZwsx520'
}

# ============ 引用解析函数 ============

QUOTE_BLOCK_PATTERN = re.compile(
    r'\[quote=(.*?)\](.*?)\[/quote\]',
    flags=re.IGNORECASE | re.DOTALL
)

def extract_quotes_and_clean(content: str):
    """
    用来解析帖子中的引用块，并去除它们在正文中的显示。

    返回 (refs, cleaned_text):
    - refs -> list，表示引用结构的层级信息（被引用作者、内容、嵌套引用）
    - cleaned_text -> 删掉 [quote=...]...[/quote] 后的文本（即纯正文）
    """
    def _extract_recursive(text):
        matches = list(QUOTE_BLOCK_PATTERN.finditer(text))
        if not matches:
            return [], text
        
        references = []
        for m in matches:
            author = m.group(1).strip()
            inner_text = m.group(2)
            nested_refs, inner_clean = _extract_recursive(inner_text)
            references.append({
                "quoted_author": author,
                "quoted_content": inner_clean.strip(),
                "quote": nested_refs
            })
        cleaned = QUOTE_BLOCK_PATTERN.sub('', text)
        return references, cleaned

    refs, out_text = _extract_recursive(content)
    out_text = re.sub(r'\n\s*\n', '\n', out_text).strip()
    return refs, out_text

# ============ 线性获取所有话题 ============

def fetch_topics(session, headers):
    """
    同步地爬取所有能获取到的主题，直到接口返回空数据为止。
    """
    url = 'https://artofproblemsolving.com/m/community/ajax.php'
    data = {
        'category_type': 'forum',
        'log_visit': '0',
        'required_tag': '',
        'fetch_before': str(int(time.time())),
        'user_id': '0',
        'fetch_archived': '0',
        'fetch_announcements': '0',
        'category_id': '463183',  # 修改为你要爬的板块ID,我们选取一个话题数量少的作为验证。https://artofproblemsolving.com/community/c463183_airsoft，这个id是463183
        'a': 'fetch_topics',
        'aops_logged_in': 'false',
        'aops_user_id': '1',
        'aops_session_id': '21d6f40cfb511982e4424e0e250a9557',
    }
    retries = 0
    max_retries = 5

    all_topics = []
    counter = 0  # 计数器，累计抓取多少条topic

    while retries < max_retries:
        logging.debug(f"正在请求，fetch_before={data['fetch_before']}")

        try:
            response = session.post(url, headers=headers, data=data, timeout=20)
        except requests.exceptions.RequestException as e:
            if "ConnectionResetError" in str(e):
                logging.error(f"[topic_id] Connection reset by peer, sleep 10s then retry: {e}")
                time.sleep(10)  # 特别等待更久
                retries += 1
            elif "Read timed out" in str(e):
                logging.error(f"[fetch_topics] Read timed out, sleep 10s then retry: {e}")
                time.sleep(10)
                retries += 1
            logging.error(f"请求异常: {e}")
            # 若还没到达 max_retries，则继续 while循环重试
            if retries >= max_retries:
                logging.error(f"[fetch_topics] 超过最大重试次数，放弃。")
                break
            else:
                continue

        if response.status_code != 200:
            logging.error(f"请求失败，状态码: {response.status_code}")
            break

        try:
            response_data = response.json()
        except ValueError:
            logging.error("响应不是有效的 JSON 格式，停止爬取。")
            logging.debug(f"原始响应内容: {response.text}")
            break

        if 'response' not in response_data:
            logging.error("响应中缺少 'response' 键。")
            logging.debug("完整响应内容: %s", json.dumps(response_data, ensure_ascii=False, indent=4))
            break

        response_content = response_data.get('response', {})
        topics = response_content.get('topics', [])
        logging.info(f"本次响应包含 {len(topics)} 个主题。")

        if not topics:
            logging.info("没有更多数据可供爬取，停止。")
            break

        # 累计添加
        for t in topics:
            tid = t.get('topic_id')
            ttitle = t.get('topic_title')
            
            # 如果接口里有 category_id, category_name，就可以这样拿：
            cid = t.get('category_id')  
            cname = t.get('category_name')

            # 如果接口里没有，就只能手动写，如：
            # cid = data['category_id']  # 如果你是强制给它设置的
            # cname = "Middle School Math"  # 根据你的需求

            if tid and ttitle:
                all_topics.append({
                    'topic_id': tid,
                    'topic_title': ttitle,
                    'category_id': cid,
                    'category_name': cname
                })
        
        # 更新计数器
        counter += len(topics)
        # 在控制台输出当前抓取进度
        print(f"已经抓到 {counter} 条 Topic。")

        # 更新 fetch_before
        last_topic = topics[-1]
        if 'last_post_time' in last_topic:
            try:
                min_timestamp = int(last_topic['last_post_time']) - 1
                data['fetch_before'] = str(min_timestamp)
            except ValueError:
                logging.error("last_post_time 不是有效的整数，停止爬取。")
                break
        else:
            logging.warning("无法找到 last_post_time，停止爬取。")
            break

        time.sleep(random.uniform(1, 2))

    # 去重
    unique_topics = {t['topic_id']: t for t in all_topics}
    all_topics = list(unique_topics.values())
    logging.info(f"总共收集到 {len(all_topics)} 个主题。")
    time.sleep(random.uniform(1, 2))  # 放慢抓取 topic 的速度
    return all_topics

# ============ 多线程抓取帖子 ============

def fetch_posts_for_topic(session, headers, topic, conn, cur_progress):
    """
    同步抓取指定 topic 下的所有帖子，并插入到数据库。
    返回本次抓取的帖子数量（可用于做统计）。
    """
    topic_id = topic['topic_id']
    url = 'https://artofproblemsolving.com/m/community/ajax.php'
    data = {
        'topic_id': topic_id,
        'direction': 'forwards',
        'start_post_id': '-1',
        'start_post_num': '1',
        'show_from_time': '-1',
        'num_to_fetch': '20',
        'a': 'fetch_posts_for_topic',
        'aops_logged_in': 'false',
        'aops_user_id': '1',
        'aops_session_id': '21d6f40cfb511982e4424e0e250a9557'
    }

    fetched_post_ids = set()
    previous_length = 0
    no_new_posts_count = 0
    retries = 0
    max_retries = 5

    cur = conn.cursor()
    total_inserted = 0  # 统计插入的帖子数量

    while retries < max_retries:
        try:
            response = session.post(url, headers=headers, data=data, timeout=20)
        except requests.exceptions.RequestException as e:
            logging.error(f"[topic_id={topic_id}] 请求异常: {e}")
            retries += 1
            time.sleep(random.uniform(1, 2))
            continue

        if response.status_code != 200:
            logging.error(f"[topic_id={topic_id}] 请求失败，状态码: {response.status_code}")
            retries += 1
            time.sleep(random.uniform(1, 2))
            continue

        try:
            response_data = response.json()
        except ValueError:
            logging.error(f"[topic_id={topic_id}] 响应非 JSON 格式。内容: {response.text}")
            break

        posts = response_data.get('response', {}).get('posts', [])
        if not posts:
            logging.info(f"[topic_id={topic_id}] 无更多帖子可爬，结束。")
            break

        for post in posts:
            post_id = post.get('post_id')
            if not post_id:
                continue
            if post_id in fetched_post_ids:
                continue
            fetched_post_ids.add(post_id)

            # 提取引用和清理正文
            raw_content = post.get('post_canonical', '')
            references, clean_content = extract_quotes_and_clean(raw_content)
            post_time_unix = post.get('post_time', 0)
            post_time_dt = datetime.datetime.utcfromtimestamp(post_time_unix)

            # ========== 新增: 取出所有额外字段 ==========
            admin = post.get('admin', False)
            attachment = post.get('attachment', False)
            avatar = post.get('avatar', '')
            deletable = post.get('deletable', False)
            deleted = post.get('deleted', False)
            editable = post.get('editable', False)
            is_forum_admin = post.get('is_forum_admin', False)
            is_nothanked = post.get('is_nothanked', False)
            is_thanked = post.get('is_thanked', False)
            last_edit_reason = post.get('last_edit_reason', '')
            last_edit_time = post.get('last_edit_time', 0)
            last_editor_username = post.get('last_editor_username', '')
            nothanks_received = post.get('nothanks_received', 0)
            num_edits = post.get('num_edits', 0)
            num_posts = post.get('num_posts', 0)
            post_canonical = post.get('post_canonical', '')
            post_format = post.get('post_format', '')
            post_number = post.get('post_number', 0)
            post_rendered = post.get('post_rendered', '')
            poster_id = post.get('poster_id', 0)
            reported = post.get('reported', False)
            show_from_end = post.get('show_from_end', False)
            show_from_start = post.get('show_from_start', False)
            thankers = post.get('thankers')
            thanks_received = post.get('thanks_received', 0)

            # 若 thankers 是 list 或 null，就转成字符串存数据库
            if thankers is not None:
                thankers_str = json.dumps(thankers, ensure_ascii=False)
            else:
                thankers_str = None


            # MySQL upsert 写法
            insert_sql = """
                INSERT INTO posts(
                    post_id, topic_id, author, content, post_time, post_references,
                    admin, attachment, avatar, deletable, deleted, editable,
                    is_forum_admin, is_nothanked, is_thanked, last_edit_reason,
                    last_edit_time, last_editor_username, nothanks_received,
                    num_edits, num_posts, post_canonical, post_format, post_number,
                    post_rendered, poster_id, reported, show_from_end, show_from_start,
                    thankers, thanks_received
                )
                VALUES (
                    %(post_id)s, %(topic_id)s, %(author)s, %(content)s, %(post_time)s, %(post_references)s,
                    %(admin)s, %(attachment)s, %(avatar)s, %(deletable)s, %(deleted)s, %(editable)s,
                    %(is_forum_admin)s, %(is_nothanked)s, %(is_thanked)s, %(last_edit_reason)s,
                    %(last_edit_time)s, %(last_editor_username)s, %(nothanks_received)s,
                    %(num_edits)s, %(num_posts)s, %(post_canonical)s, %(post_format)s, %(post_number)s,
                    %(post_rendered)s, %(poster_id)s, %(reported)s, %(show_from_end)s, %(show_from_start)s,
                    %(thankers)s, %(thanks_received)s
                )
                ON DUPLICATE KEY UPDATE
                    topic_id = VALUES(topic_id),
                    author = VALUES(author),
                    content = VALUES(content),
                    post_time = VALUES(post_time),
                    post_references = VALUES(post_references),
                    admin = VALUES(admin),
                    attachment = VALUES(attachment),
                    avatar = VALUES(avatar),
                    deletable = VALUES(deletable),
                    deleted = VALUES(deleted),
                    editable = VALUES(editable),
                    is_forum_admin = VALUES(is_forum_admin),
                    is_nothanked = VALUES(is_nothanked),
                    is_thanked = VALUES(is_thanked),
                    last_edit_reason = VALUES(last_edit_reason),
                    last_edit_time = VALUES(last_edit_time),
                    last_editor_username = VALUES(last_editor_username),
                    nothanks_received = VALUES(nothanks_received),
                    num_edits = VALUES(num_edits),
                    num_posts = VALUES(num_posts),
                    post_canonical = VALUES(post_canonical),
                    post_format = VALUES(post_format),
                    post_number = VALUES(post_number),
                    post_rendered = VALUES(post_rendered),
                    poster_id = VALUES(poster_id),
                    reported = VALUES(reported),
                    show_from_end = VALUES(show_from_end),
                    show_from_start = VALUES(show_from_start),
                    thankers = VALUES(thankers),
                    thanks_received = VALUES(thanks_received)
            """
            params = {
                'post_id': int(post_id),
                'topic_id': int(topic_id),
                'author': post.get('username'),
                'content': clean_content,
                'post_time': post_time_dt,
                'post_references': json.dumps(references, ensure_ascii=False),

                'admin': admin,
                'attachment': attachment,
                'avatar': avatar,
                'deletable': deletable,
                'deleted': deleted,
                'editable': editable,
                'is_forum_admin': is_forum_admin,
                'is_nothanked': is_nothanked,
                'is_thanked': is_thanked,
                'last_edit_reason': last_edit_reason,
                'last_edit_time': last_edit_time,
                'last_editor_username': last_editor_username,
                'nothanks_received': nothanks_received,
                'num_edits': num_edits,
                'num_posts': num_posts,
                'post_canonical': post_canonical,
                'post_format': post_format,
                'post_number': post_number,
                'post_rendered': post_rendered,
                'poster_id': poster_id,
                'reported': reported,
                'show_from_end': show_from_end,
                'show_from_start': show_from_start,
                'thankers': thankers_str,
                'thanks_received': thanks_received
            }




            try:
                cur.execute(insert_sql, params)
                if cur.rowcount > 0:
                    total_inserted += 1

            except Exception as e:
                logging.error(f"[topic_id={topic_id}] 插入帖子 {post_id} 失败: {e}")

        conn.commit()

        data['start_post_num'] = str(int(data['start_post_num']) + len(posts))

        if len(fetched_post_ids) == previous_length:
            no_new_posts_count += 1
        else:
            no_new_posts_count = 0
        previous_length = len(fetched_post_ids)

        # 如果连续两次没有新帖子，就退出
        if no_new_posts_count >= 2:
            logging.info(f"[topic_id={topic_id}] 连续2次无新帖子，结束。")
            break

        logging.debug(f"[topic_id={topic_id}] 本批新增 {len(posts)} 条帖子, 累计爬取 {len(fetched_post_ids)}")
        time.sleep(random.uniform(1, 2))

    if retries >= max_retries:
        logging.error(f"[topic_id={topic_id}] 超过最大重试次数，放弃。")

    # 爬完后更新 progress

    # 原逻辑是 "ON CONFLICT DO NOTHING", 用 MySQL 可用 INSERT IGNORE or ON DUP KEY
    try:
        cur_progress.execute(
            "INSERT IGNORE INTO progress(topic_id) VALUES (%s)",
            (topic_id,)
        )
        conn.commit()
    except Exception as e:
        logging.error(f"[topic_id={topic_id}] 插入进度表失败: {e}")

    cur.close()
    return total_inserted

# ============ 主函数 ============

def main():
    start_time = time.time()

    # 建立 MySQL 连接
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        logging.info("MySQL 数据库连接成功。")
    except mysql.connector.Error as e:
        logging.error(f"数据库连接失败: {e}")
        return

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Cookie': ('optimizelyEndUserId=oeu1731650345778r0.5366359490728461; '
                   '_fbp=fb.1.1731650347330.442242167359780878; '
                   '_ga=GA1.1.913098029.1731650348; '
                   '_hjSessionUser_774800=eyJpZCI6IjJkNmRjNjEwLTYyMjItNWNlZi1iMGViLTI4ZDg3OTQyYjlkZiIsImNyZWF0ZWQiOjE3MzE2NTAzNDg4NDcsImV4aXN0aW5nIjp0cnVlfQ==; '
                   '_gcl_au=1.1.528690174.1731650348.430118910.1731650475.1731650475; '
                   'ajax_token_register_key=32123124-3d4a-4b7d-adca-f54e37b02320; '
                   'captcha_key=28057ea3-5066-4d93-81e9-890b3768d219; '
                   'platsessionid__expires=1737875478163; '
                   'f2=8daebfd29e92859b4e64b52743a3b7a9715220bd3cdc9e14fe42669f97b905104ddd93543050f09ad102912304933c775e9425533e933fc53f79f882357b625e; '
                   '_uetsid=ed6c5830abdd11ef9c03af3936158ecf; '
                   '_uetvid=b9c8d580a31611ef9143fbfccf8cf553; '
                   'cmty_init_time=1732782104'),
        'Origin': 'https://artofproblemsolving.com',
        'Referer': 'https://artofproblemsolving.com/',
        'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'),
        'X-Requested-With': 'XMLHttpRequest',
    }

    session = requests.Session()
    session.trust_env = False

    # -- 1. 读取已完成的 topic_id
    done_topic_ids = set()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT topic_id FROM progress;")
            rows = cur.fetchall()
            for r in rows:
                done_topic_ids.add(str(r[0]))
    except Exception as e:
        logging.error(f"读取 progress 表失败: {e}")
        pass

    # -- 2. 抓取所有 topic
    all_topics = fetch_topics(session, headers)
    logging.info(f"抓到 {len(all_topics)} 个主题。")

    # -- 3. 插/更新 topics
    try:
        with conn.cursor() as cur:
            insert_topics_sql = """
                INSERT INTO topics(topic_id, topic_title, category_id, category_name)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                  topic_title = VALUES(topic_title),
                  category_id = VALUES(category_id),
                  category_name = VALUES(category_name)
            """
            for t in all_topics:
                cur.execute(
                    insert_topics_sql,
                    (
                        int(t['topic_id']),
                        t['topic_title'],
                        t.get('category_id'),
                        t.get('category_name')
                    )
                )
        conn.commit()
    except Exception as e:
        logging.error(f"插入 topics 表时出错: {e}")
        session.close()
        conn.close()
        return

    # -- 4. 过滤已爬取的
    topics_to_crawl = [t for t in all_topics if t['topic_id'] not in done_topic_ids]
    logging.info(f"有 {len(topics_to_crawl)} 个主题需要新抓取。")

    # -- 5. 多线程抓取帖子
    max_threads = 5
    total_posts_inserted = 0  

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        cur_progress = conn.cursor()
        futures = []
        for topic in topics_to_crawl:
            future = executor.submit(
                fetch_posts_for_topic, session, headers, topic, conn, cur_progress
            )
            futures.append(future)

        for future in tqdm(as_completed(futures), total=len(futures), desc="抓取帖子"):
            try:
                inserted_count = future.result()
                total_posts_inserted += inserted_count
            except Exception as exc:
                logging.error(f"抓取线程出现异常: {exc}")

        conn.commit()
        cur_progress.close()

    session.close()
    conn.close()

    end_time = time.time()
    elapsed_time = end_time - start_time
    hours, rem = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(rem, 60)
    formatted_time = f"{int(hours)}小时 {int(minutes)}分钟 {int(seconds)}秒"

    print(f"爬取完成：共获取 {len(all_topics)} 个主题，新增抓取 {len(topics_to_crawl)} 个，插入帖子 {total_posts_inserted} 条。")
    print(f"程序运行时间: {formatted_time}")
    logging.info(f"爬虫结束，插入帖子总数={total_posts_inserted}；程序耗时: {formatted_time}")

if __name__ == "__main__":
    main()