import requests
import json
import time
import random
import logging
import datetime
import re
import mysql.connector
import os
from threading import Thread
from queue import Queue
import sys

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


# ============ 线性获取所有话题 ============

def insert_topics_batch(conn, topics):
    """
    插入一批topics到数据库
    """
    insert_sql = """
        INSERT INTO topics(topic_id, topic_title, category_id, category_name)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          topic_title = VALUES(topic_title),
          category_id = VALUES(category_id),
          category_name = VALUES(category_name)
    """
    cur = conn.cursor()
    for t in topics:
        cur.execute(insert_sql, (
            t['topic_id'], t['topic_title'],
            t.get('category_id'), t.get('category_name')
        ))
    conn.commit()
    cur.close()

def fetch_topics_producer(session, headers, topic_queue):
    """
    边抓topics边写数据库，并将新的topics放进队列供后续爬帖子。
    同时读取progress表，跳过已经完成（爬过）的topic_id，实现断点续抓。
    """
    # 建立连接
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # 1) 读取progress表，获取已经处理完的topic_id
    done_ids = set()
    try:
        cur.execute("SELECT topic_id FROM progress")
        rows = cur.fetchall()
        for r in rows:
            done_ids.add(r[0])
        logging.info(f"progress表中已有 {len(done_ids)} 个已完成的topic。")
    except Exception as e:
        logging.error(f"读取 progress 表失败: {e}")
        
        
    # 准备抓 topic
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
    total_count = 0

    while retries < max_retries:
        try:
            response = session.post(url, headers=headers, data=data, timeout=20)
            response_data = response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"fetch_topics 请求异常: {e}")
            retries += 1
            time.sleep(5)
            continue
        except ValueError:
            logging.error("响应不是 JSON 格式")
            break

        content = response_data.get('response', {})
        topics = content.get('topics', [])
        if not topics:
            logging.info("没有更多 topic 数据，停止。")
            break
        
        # 2) 先插数据库
        insert_topics_batch(conn, topics)
        total_count += len(topics)
        logging.info(f"本次抓到 {len(topics)} 个topic，累计{total_count}")

        # 3) 跳过progress里已有的，剩下的放队列
        new_count = 0
        for t in topics:
            tid = t['topic_id']
            if tid not in done_ids:
                topic_queue.put(t)
                new_count += 1
        logging.info(f"其中 {new_count} 个topic是新需要抓帖子的。")

        # 4) 翻页
        last_topic = topics[-1]
        if 'last_post_time' in last_topic:
            data['fetch_before'] = str(int(last_topic['last_post_time']) - 1)
        else:
            break

        time.sleep(random.uniform(1, 2))

    logging.info(f"Topic 抓取结束, 共获取 {total_count} 个topic(包含已完成的)。")
    cur.close()
    conn.close()   # 通知消费者没有更多新topic


# ============ Consumer：抓帖子逻辑(不变，大体) ============
def fetch_posts_worker(topic_queue, session, headers):
    """
    消费者：不断从 topic_queue 里拿出一个 topic，抓该 topic 的posts
    并边写 posts表 (每个worker自己开/关MySQL连接)。
    """
    while True:
        topic = topic_queue.get()
        if topic is None:
            # 表示没有更多topic
            topic_queue.task_done()
            break

        try:
            # 每个worker独立连数据库
            conn = mysql.connector.connect(**DB_CONFIG)
            cur = conn.cursor()

            # === 在这抓帖子 ===
            fetch_posts_for_topic(topic, session, headers, conn)

            cur.close()
            conn.close()
        except Exception as e:
            logging.error(f"fetch_posts_worker 异常: {e}")
        finally:
            topic_queue.task_done()

    
    
 

# ============ 多线程抓取帖子 ============

def fetch_posts_for_topic(topic, session, headers, conn):
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
                    post_id, topic_id, author, post_canonical, post_time, 
                    admin, attachment, avatar, deletable, deleted, editable,
                    is_forum_admin, is_nothanked, is_thanked, last_edit_reason,
                    last_edit_time, last_editor_username, nothanks_received,
                    num_edits, num_posts,  post_format, post_number,
                    post_rendered, poster_id, reported, show_from_end, show_from_start,
                    thankers, thanks_received
                )
                VALUES (
                    %(post_id)s, %(topic_id)s, %(author)s, %(post_canonical)s, %(post_time)s, 
                    %(admin)s, %(attachment)s, %(avatar)s, %(deletable)s, %(deleted)s, %(editable)s,
                    %(is_forum_admin)s, %(is_nothanked)s, %(is_thanked)s, %(last_edit_reason)s,
                    %(last_edit_time)s, %(last_editor_username)s, %(nothanks_received)s,
                    %(num_edits)s, %(num_posts)s,  %(post_format)s, %(post_number)s,
                    %(post_rendered)s, %(poster_id)s, %(reported)s, %(show_from_end)s, %(show_from_start)s,
                    %(thankers)s, %(thanks_received)s
                )
                ON DUPLICATE KEY UPDATE
                    topic_id = VALUES(topic_id),
                    author = VALUES(author),
                    post_canonical = VALUES(post_canonical),
                    post_time = VALUES(post_time),
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
                'post_canonical': post_canonical, 
                'post_time': post_time_dt,
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
        
        logging.info(f"[topic_id={topic_id}] 开始抓posts...")

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
      with conn.cursor() as c:
        c.execute("INSERT IGNORE INTO progress(topic_id) VALUES(%s)", (topic_id,))
      conn.commit()
    except Exception as e:
      logging.error(f"[topic_id={topic_id}] 插入进度表失败: {e}")

    cur.close()
    return total_inserted
  

# ============ 主函数 ============

def main():
    start_time = time.time()
    
    # 先建session
    session = requests.Session()
    session.trust_env = False

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
    # 准备一个队列用于topics
    topic_queue = Queue()
    # 启动Producer   
    # 1) 启动一个线程去抓topic + 写topic表 + 往队列放topic
    producer_thread = Thread(target=fetch_topics_producer, args=(session, headers, topic_queue))
    producer_thread.start()
    # 启动多个Consumer
    # 2) 启动多个worker线程，从队列拿topic，抓posts并写库
    worker_count = 5
    worker_threads = []
    for _ in range(worker_count):
        t = Thread(target=fetch_posts_worker, args=(topic_queue, session, headers))
        t.start()
        worker_threads.append(t)

    # 3) 等producer结束
    producer_thread.join()

    # 这里确定producer结束后，你也可以再放够数量的None
    for _ in range(worker_count):
        topic_queue.put(None)

    # 4) 等worker把队列里的topic处理完
    # topic_queue.join()  # 如果你要确保全部处理完再往下

    for t in worker_threads:
        t.join()

    end_time = time.time()
    elapsed = end_time - start_time
    h, rem = divmod(elapsed, 3600)
    m, s = divmod(rem, 60)
    logging.info(f"总耗时 {int(h)}小时{int(m)}分钟{int(s)}秒.")
    print("Done.")

    

if __name__ == "__main__":
    main()