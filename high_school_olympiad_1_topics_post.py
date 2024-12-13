import asyncio
import aiohttp
import aiofiles
import json
import time
import random
import logging
import os
from tqdm.asyncio import tqdm  # 使用 tqdm 的异步版本
import datetime

# 获取脚本所在的绝对路径
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# 设置日志记录，日志文件位于脚本所在目录
logging.basicConfig(
    filename=os.path.join(SCRIPT_DIR, 'async_crawler.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 定义全局变量，用于控制并发数量
CONCURRENT_REQUESTS = 200  # 可根据需要调整
BATCH_SIZE = 1000  # 每个文件保存的主题数量

# 进度文件的绝对路径，位于脚本所在目录
PROGRESS_FILE = os.path.join(SCRIPT_DIR, 'progress.json')


async def fetch_topics(session, headers, desired_count):
    """
    异步爬取指定数量的主题，返回每个主题的 topic_id 和 topic_title。
    """
    url = 'https://artofproblemsolving.com/m/community/ajax.php'

    data = {
        'category_type': 'forum',
        'log_visit': '1',
        'required_tag': '',
        'fetch_before': str(int(time.time())),  # 使用当前时间戳作为起始点
        #'fetch_before': '1597788054', # 报错检查点，断点续传
        'user_id': '0',
        'fetch_archived': '0',
        'fetch_announcements': '0',
        'category_id': '4',
        'a': 'fetch_topics',
        'aops_logged_in': 'false',
        'aops_user_id': '1',
        'aops_session_id': '21d6f40cfb511982e4424e0e250a9557',  # 请确保 session_id 有效
    }

    all_topics = []
    current_count = 0

    while current_count < desired_count:
        logging.info(f"正在请求，fetch_before: {data['fetch_before']}")
        print(f"正在请求，fetch_before: {data['fetch_before']}")

        try:
            async with session.post(url, headers=headers, data=data) as response:
                if response.status != 200:
                    logging.error(f"请求失败，状态码: {response.status}")
                    print(f"请求失败，状态码: {response.status}")
                    break

                try:
                    response_data = await response.json(content_type=None)
                except aiohttp.ContentTypeError:
                    logging.error("无法解析 JSON 响应。")
                    print("无法解析 JSON 响应。")
                    break

                if 'response' not in response_data:
                    logging.error("响应中缺少 'response' 键。")
                    print("响应中缺少 'response' 键。")
                    break

                response_content = response_data.get('response', {})
                topics = response_content.get('topics', [])

                logging.info(f"响应包含 {len(topics)} 个主题。")
                print(f"响应包含 {len(topics)} 个主题。")

                if not topics:
                    logging.warning("没有更多的数据可供爬取。")
                    print("没有更多的数据可供爬取。")
                    break

                # 打印第一个主题的内容以确认结构
                # if topics:
                #     first_topic = topics[0]
                #     logging.info(f"第一个主题的内容：{json.dumps(first_topic, ensure_ascii=False, indent=4)}")
                #     print(f"第一个主题的内容：{json.dumps(first_topic, ensure_ascii=False, indent=4)}")

                for topic in topics:  # 确认 'topic_id' 和 'topic_title' 是否存在
                    if 'topic_id' in topic and 'topic_title' in topic:
                        all_topics.append({
                            'topic_id': topic['topic_id'],
                            'topic_title': topic['topic_title'],
                            'posts': []  # 初始化帖子列表
                        })
                        current_count += 1
                        if current_count >= desired_count:
                            break
                    else:
                        logging.warning(f"主题结构不符合预期: {topic}")
                        print(f"主题结构不符合预期: {topic}")

                print(f"已收集 {current_count} 个主题。")
                logging.info(f"已收集 {current_count} 个主题。")

                # 获取最后一个帖子的 last_post_time
                if topics:
                    last_topic = topics[-1]
                    if 'last_post_time' in last_topic:
                        try:
                            min_timestamp = int(last_topic['last_post_time']) - 1
                            data['fetch_before'] = str(min_timestamp)
                            logging.info(f"更新 fetch_before 为: {data['fetch_before']}")
                            print(f"更新 fetch_before 为: {data['fetch_before']}")
                        except ValueError:
                            logging.error("last_post_time 不是有效的整数，停止爬取。")
                            print("last_post_time 不是有效的整数，停止爬取。")
                            break
                    else:
                        logging.warning("无法找到 last_post_time，停止爬取。")
                        print("无法找到 last_post_time，停止爬取。")
                        break

                # 减少不必要的延迟
                await asyncio.sleep(random.uniform(0.1, 0.3))

        except Exception as e:
            logging.error(f"请求异常: {e}")
            print(f"请求异常: {e}")
            break

    # 去重
    unique_topics = {topic['topic_id']: topic for topic in all_topics}
    all_topics = list(unique_topics.values())

    # 如果收集的主题超过需要的数量，进行截取
    if len(all_topics) > desired_count:
        all_topics = all_topics[:desired_count]

    logging.info(f"共收集到 {len(all_topics)} 个主题。")
    print(f"共收集到 {len(all_topics)} 个主题。")

    return all_topics

def extract_all_quotes(content, pos=0):
    """
    提取多级嵌套引用，并返回引用结构和正文内容。
    """
    quotes = []
    text = ''
    length = len(content)

    while pos < length:
        if content.startswith('[quote=', pos):
            # 找到作者名的结束位置
            end_author = content.find(']', pos)
            if end_author == -1:
                # 如果没有找到']'，说明格式不正确，跳出循环
                break
            author = content[pos+7:end_author]
            pos = end_author + 1  # 移动到作者名之后的位置

            # 递归解析引用内容
            nested_quotes, quoted_text = extract_all_quotes(content, pos)

            # 构建引用结构
            quotes.append({
                "quoted_author": author.strip(),
                "quoted_content": quoted_text.strip(),
                "quote": nested_quotes
            })

            # 更新 pos 到引用结束位置
            end_quote_pos = content.find('[/quote]', pos)
            if end_quote_pos == -1:
                break  # 如果没有找到结束标签，跳出循环
            pos = end_quote_pos + 8  # 跳过 '[/quote]'
        elif content.startswith('[/quote]', pos):
            pos += 8  # 移动到 '[/quote]' 之后的位置
            return quotes, text.strip()
        else:
            # 查找下一个标签的位置
            next_quote_pos = content.find('[quote=', pos)
            next_endquote_pos = content.find('[/quote]', pos)
            next_tag_pos_list = [p for p in [next_quote_pos, next_endquote_pos] if p != -1]

            if next_tag_pos_list:
                next_tag_pos = min(next_tag_pos_list)
                # 收集文本直到下一个标签
                text += content[pos:next_tag_pos]
                pos = next_tag_pos
            else:
                # 没有更多的标签，收集剩余的文本
                text += content[pos:]
                pos = length

    return quotes, text.strip()

async def fetch_posts_for_topic(session, headers, topic, semaphore, progress_lock):
    """
    异步获取单个主题的帖子信息，并返回主题数据。
    """
    async with semaphore:
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
            'aops_session_id': '21d6f40cfb511982e4424e0e250a9557'  # 确保 session_id 有效
        }

        all_posts = []
        fetched_post_ids = set()  # 记录已爬取的 post_id
        previous_length = 0
        no_new_posts_count = 0  # 初始化计数器
        retries = 0  # 重试计数
        max_retries = 3 

        while retries < max_retries:
            try:
                async with session.post(url, headers=headers, data=data) as response:
                    if response.status != 200:
                        logging.error(f"请求失败 (topic_id: {topic_id}), 状态码: {response.status}")
                        print(f"请求失败 (topic_id: {topic_id}), 状态码: {response.status}")
                        retries += 1
                        await asyncio.sleep(random.uniform(0.1, 0.3))
                        continue

                    try:
                        response_data = await response.json(content_type=None)
                    except aiohttp.ContentTypeError:
                        logging.error(f"无法解析 JSON 响应 (topic_id: {topic_id})")
                        print(f"无法解析 JSON 响应 (topic_id: {topic_id})")
                        # 打印原始响应内容以便调试
                        raw_text = await response.text()
                        logging.debug(f"原始响应内容 (topic_id: {topic_id}): {raw_text}")
                        print(f"原始响应内容: {raw_text}")
                        break

                    response_section = response_data.get('response', {})
                    posts = response_section.get('posts', [])  

                    if not posts:
                        logging.info(f"所有帖子已爬取完成 (topic_id: {topic_id})。")
                        print(f"所有帖子已爬取完成 (topic_id: {topic_id})。")
                        break

                    for post in posts:
                        post_id = post.get('post_id')
                        if post_id and post_id not in fetched_post_ids:
                            fetched_post_ids.add(post_id)
                            author = post.get('username')
                            content = post.get('post_canonical', '')
                            post_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(post.get('post_time', 0)))
                            # 调用改进的引用解析函数   
                            references, clean_content = extract_all_quotes(content)
                            # 存储帖子信息
                            all_posts.append({
                                'post_id': post_id,
                                'author': author,
                                'content': clean_content,  # 去掉引用后的正文
                                'time': post_time,
                                'references': references  # 引用结构
                            })
                        else:
                            logging.info(f"重复的帖子已跳过 (topic_id: {topic_id}): post_id = {post_id}")
                            print(f"重复的帖子已跳过 (topic_id: {topic_id}): post_id = {post_id}")
                    
                    # 更新请求参数
                    data['start_post_num'] = str(int(data['start_post_num']) + len(posts))

                    # 检查是否有新增帖子
                    if len(all_posts) == previous_length:
                        no_new_posts_count += 1
                        logging.info(f"连续 {no_new_posts_count} 次没有新增帖子 (topic_id: {topic_id})。")
                        print(f"连续 {no_new_posts_count} 次没有新增帖子 (topic_id: {topic_id})。")
                    else:
                        no_new_posts_count = 0  # 重置计数器

                    # 更新 previous_length
                    previous_length = len(all_posts)

                    # 如果连续 2 次没有新增帖子，跳出循环
                    if no_new_posts_count >= 2:
                        logging.info(f"连续 2 次没有新增帖子，结束爬取 (topic_id: {topic_id})。")
                        print(f"连续 2 次没有新增帖子，结束爬取 (topic_id: {topic_id})。")
                        break

                    logging.info(f"已爬取 {len(posts)} 条帖子，总计 {len(all_posts)} 条 (topic_id: {topic_id})。")
                    print(f"已爬取 {len(posts)} 条帖子，总计 {len(all_posts)} 条 (topic_id: {topic_id})。")

                    await asyncio.sleep(random.uniform(0.1, 0.3))

            except Exception as e:
                logging.error(f"请求异常 (topic_id: {topic_id}): {e}")
                print(f"请求异常 (topic_id: {topic_id}): {e}")
                retries += 1
                await asyncio.sleep(random.uniform(0.1, 0.3))
                continue

        # 如果超过最大重试次数，记录失败
        if retries >= max_retries:
            logging.error(f"主题 {topic_id} 抓取失败，超过最大重试次数。")
            print(f"主题 {topic_id} 抓取失败，超过最大重试次数。")
            return None

        # 去重并去掉 post_id
        unique_posts = {post['post_id']: post for post in all_posts}
        all_posts = list(unique_posts.values())

        filtered_posts = [
            {key: post[key] for key in post if key != 'post_id'}  # 去掉 post_id
            for post in all_posts
        ]

        topic['posts'] = filtered_posts
        logging.info(f"主题 {topic_id} 的所有帖子已爬取完成，总共 {len(filtered_posts)} 条帖子。")
        print(f"主题 {topic_id} 的所有帖子已爬取完成，总共 {len(filtered_posts)} 条帖子。")

        # 更新进度文件
        await update_progress(topic['topic_id'], progress_lock)

        return topic

async def update_progress(topic_id, progress_lock):
    """
    更新进度文件，记录已完成的 topic_id。
    """
    async with progress_lock:
        progress = set()
        if os.path.exists(PROGRESS_FILE):
            try:
                async with aiofiles.open(PROGRESS_FILE, 'r') as f:
                    data = await f.read()
                    progress = set(json.loads(data))
            except Exception as e:
                logging.error(f"读取进度文件时出错: {e}")
                print(f"读取进度文件时出错: {e}")

        progress.add(topic_id)

        try:
            async with aiofiles.open(PROGRESS_FILE, 'w') as f:
                await f.write(json.dumps(list(progress)))
        except Exception as e:
            logging.error(f"写入进度文件时出错: {e}")
            print(f"写入进度文件时出错: {e}")

async def async_main(desired_count):
    # 记录开始时间
    start_time = time.time()

    # 定义请求的 Headers
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
                   '_hjSession_774800=eyJpZCI6ImU4Y2I3N2Q2LTZmMTQtNDdhMi1hZjA4LTQ3MDhiNGFmMDBiMiIsImMiOjE3MzI4NDgwMDIwNjksInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MX0=; '
                   '_uetsid=ed6c5830abdd11ef9c03af3936158ecf; '
                   '_uetvid=b9c8d580a31611ef9143fbfccf8cf553; '
                   'cmty_init_time=1732782104'),
        'Origin': 'https://artofproblemsolving.com',
        'Priority': 'u=1, i',
        'Referer': 'https://artofproblemsolving.com/',
        'Sec-CH-UA': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'Sec-CH-UA-Mobile': '?0',
        'Sec-CH-UA-Platform': '"macOS"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'),
        'X-Requested-With': 'XMLHttpRequest',
    }

    # 创建异步会话
    connector = aiohttp.TCPConnector(limit_per_host=CONCURRENT_REQUESTS, ssl=False)
    timeout = aiohttp.ClientTimeout(total=60)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # 步骤1：获取 topic_id 和 topic_title
        topics = await fetch_topics(session, headers, desired_count)
        logging.info(f"共获取到 {len(topics)} 个主题。")
        print(f"共获取到 {len(topics)} 个主题。")

        if not topics:
            print("未获取到任何主题，程序结束。")
            return

        # 读取已完成的 topic_id 列表
        completed_topic_ids = set()
        if os.path.exists(PROGRESS_FILE):
            try:
                async with aiofiles.open(PROGRESS_FILE, 'r') as f:
                    data = await f.read()
                    completed_topic_ids = set(json.loads(data))
            except Exception as e:
                logging.error(f"读取进度文件时出错: {e}")
                print(f"读取进度文件时出错: {e}")

        # 过滤未完成的主题
        remaining_topics = [topic for topic in topics if topic['topic_id'] not in completed_topic_ids]
        logging.info(f"剩余需要爬取的主题数量: {len(remaining_topics)}")
        print(f"剩余需要爬取的主题数量: {len(remaining_topics)}")

        if not remaining_topics:
            print("所有主题均已完成爬取。")
            return
        # 在事件循环中创建锁
        progress_lock = asyncio.Lock()

        # 步骤2：使用异步任务获取每个主题的帖子信息
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        tasks = [
        fetch_posts_for_topic(session, headers, topic, semaphore, progress_lock)
        for topic in remaining_topics
        ]

        results = []
        async for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="抓取帖子"):
            result = await f
            if result is not None:
                results.append(result)

        # 步骤3：保存所有数据到 JSON 文件
        # 统计总共爬取的帖子数量
        total_posts = sum(len(topic['posts']) for topic in results)
        total_topics = len(results)
        print(f"总共爬取了 {total_topics} 个话题，{total_posts} 个帖子。")
        logging.info(f"总共爬取了 {total_topics} 个话题，{total_posts} 个帖子。")

        if total_topics == 0:
            print("没有成功爬取任何主题。")
            return

        # 动态生成文件名，包含日期、主题数量和帖子数量
        current_datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_index = 1
        batch = []
        for idx, topic in enumerate(results, 1):
            batch.append(topic)
            if idx % BATCH_SIZE == 0:
                # 计算当前批次的主题数量和帖子数量
                batch_topic_count = len(batch)
                batch_post_count = sum(len(t['posts']) for t in batch)
                file_name = f"{current_datetime}_batch_{file_index}_topics_{batch_topic_count}_posts_{batch_post_count}.json"
                file_path = os.path.join(SCRIPT_DIR, file_name)  # 构建绝对路径
                async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
                    await f.write(json.dumps(batch, ensure_ascii=False, indent=4))
                logging.info(f"已保存 {batch_topic_count} 个主题，{batch_post_count} 个帖子到 {file_path}。")
                print(f"已保存 {batch_topic_count} 个主题，{batch_post_count} 个帖子到 {file_path}。")
                batch = []
                file_index += 1

        # 保存剩余的主题
        if batch:
            file_name = f"{current_datetime}_topics_batch_{file_index}.json"
            file_path = os.path.join(SCRIPT_DIR, file_name)  # 构建绝对路径
            async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(batch, ensure_ascii=False, indent=4))
            logging.info(f"已保存 {len(batch)} 个主题到 {file_path}。")
            print(f"已保存 {len(batch)} 个主题到 {file_path}。")

        # 记录结束时间
        end_time = time.time()
        elapsed_time = end_time - start_time

        # 格式化运行时间
        hours, rem = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(rem, 60)
        formatted_time = f"{int(hours)}小时 {int(minutes)}分钟 {int(seconds)}秒"

        print(f"爬取完成，共获取了 {total_topics} 个主题。")
        print(f"程序运行时间: {formatted_time}")
        logging.info(f"爬取完成，共获取了 {total_topics} 个主题。")
        logging.info(f"程序运行时间: {formatted_time}")

def main():
    # 获取用户输入
    try:
        desired_count = int(input("请输入需要爬取的主题数量: "))
        if desired_count <= 0:
            raise ValueError("主题数量必须是正整数。")
    except ValueError as ve:
        print(f"输入错误: {ve}")
        return

    # 运行异步主函数
    asyncio.run(async_main(desired_count))

if __name__ == "__main__":
    main()