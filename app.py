import os
import time
from chalice import Chalice, Cron
import boto3
from bs4 import BeautifulSoup
import requests
from botocore.exceptions import ClientError

app = Chalice(app_name='news-notifier')


def scrape_natalie(data: dict):
    """ナタリーからニュースリンクを収集する"""
    site_name = "ナタリー"
    data[site_name] = {}
    url = 'https://natalie.mu/news/'
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    soup = soup.find('div', class_='NA_section')
    news_cards = soup.find_all('div', class_='NA_card')
    for card in news_cards:
        title = card.find('p', class_='NA_card_title').get_text()
        # ここでaタグで特定できればいいが、タグリンクのaタグが含まれているためaタグを一個ずつ調べる
        news_url = None
        for a in card.find_all('a'):
            # urlにnewsが含まれるのが記事リンク
            if 'news' in a.get('href'):
                news_url = a.get('href')
                break
        if news_url:
            data[site_name][news_url] = title
    return data


def is_url_processed(table, url, site_name):
    """DynamoDBに登録済みか調べる"""
    try:
        response = table.get_item(
            Key={
                'news_url': url,
                'site_name': site_name
            }
        )
        return 'Item' in response
    except ClientError as e:
        # エラーが発生した場合は、Trueを返す
        return True


def prepare_notification(news):
    """通知メッセージを準備"""
    message = "新着ニュース:\n\n"
    for site_name, articles in news.items():
        message += f"{site_name}:\n\n"
        for title, url in articles.items():
            message += f"- {title}\n  {url}\n\n"
        message += "\n\n"
    return message


def save_to_dynamodb(table, news):
    """DynamoDBにアイテムを保存"""
    current_time = int(time.time())
    one_week_later = current_time + (7 * 24 * 60 * 60)
    for site_name, articles in news.items():
        for url, title in articles.items():
            try:
                table.put_item(
                    Item={
                        'news_url': url,
                        'site_name': site_name,
                        'title': title,
                        'timestamp': current_time,
                        'expiration_time': one_week_later
                    }
                )
            except ClientError as e:
                if e.response['Error']['Code'] == 'ValidationException':
                    continue
                else:
                    # 他のClientErrorの場合は再raise
                    raise


# 毎日 8:30 11:30 14:30 17:30に実行
# UTCなので、9H前で指定
@app.schedule(Cron(minutes='30', hours='23,2,5,8,11', day_of_month='?', month='*', day_of_week='*', year='*'))
def scrape_and_notify(event):
    """メイン処理"""
    # DynamoDBクライアントの設定
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ.get('DYNAMODB_TABLE'))

    # SNSクライアントの設定
    sns = boto3.client('sns')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
    new_news = {}
    # 新しいニュースをスクレイピング
    new_news = scrape_natalie(new_news)
    # 配信するニュース
    news_subscribe = {}
    for site_name, articles in new_news.items():
        found = False
        d = {}
        for url, title in articles.items():
            # 既にDynamoDBに登録済みだったら配信済みなのでスキップ
            if is_url_processed(table, url, site_name):
                continue
            found = True
            d[url] = title
        if found:
            news_subscribe[site_name] = d
    if news_subscribe:
        # SNS通知のテキストを作る
        notification_message = prepare_notification(news_subscribe)
        # SNS通知する
        if notification_message:
            sns.publish(
                TopicArn=sns_topic_arn,
                Message=notification_message
            )

        # DynamoDBへの保存
        save_to_dynamodb(table, news_subscribe)
