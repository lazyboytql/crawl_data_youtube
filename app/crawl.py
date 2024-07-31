import os
import googleapiclient.discovery
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime

API_KEY = os.getenv("API_KEY", "AIzaSyBAnDCX6u1ya5cHQQzN9rQdn43ECzr3pvY")

DB_PARAMS = {
    'dbname': os.getenv("DB_NAME", "your DB postgres name"),
    'user': os.getenv("DB_USER", "Your User"),
    'password': os.getenv("DB_PASSWORD", "Your Password"),
    'host': os.getenv("DB_HOST", "localhost"),
    'port': os.getenv("DB_PORT", "5432")
}

def get_video_comments(video_id, max_results=100):
    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=API_KEY)

    comments = []
    next_page_token = None

    while True:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=max_results,
                pageToken=next_page_token
            )
            response = request.execute()

            for item in response['items']:
                comment = item['snippet']['topLevelComment']['snippet']
                text = comment['textDisplay'].lower()

                comments.append((
                    video_id,
                    comment['authorDisplayName'].lower(),
                    text,
                    comment['likeCount'],
                    convert_time(comment['publishedAt']),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S') 
                ))

            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
        except Exception as e:
            print(f"An error occurred: {e}")
            break
    return comments

def convert_time(time_published):
    try:
        dt = datetime.strptime(time_published, '%Y-%m-%dT%H:%M:%SZ')
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return time_published

def save_to_postgres(data, batch_size=100):
    insert_query = """
        INSERT INTO youtube_comments (video_id, author, text, like_count, published_at, inserted_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (video_id, author, text, published_at) DO NOTHING
    """

    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    execute_batch(cur, insert_query, batch)
                    conn.commit()
    except Exception as e:
        print(f"Database error: {e}")

def get_videos_from_channel(channel_id, max_results=10):
    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=API_KEY)
    videos = []

    request = youtube.search().list(
        part="id",
        channelId=channel_id,
        maxResults=max_results,
        order="date"
    )

    response = request.execute()
    for item in response['items']:
        if item['id']['kind'] == 'youtube#video':
            videos.append(item['id']['videoId'])

    return videos

def main():
    channel_id = "Your channel_id"
    video_ids = get_videos_from_channel(channel_id, max_results=10)

    for video_id in video_ids:
        comments = get_video_comments(video_id)
        if comments:
            save_to_postgres(comments)
            print(f"Saved {len(comments)} comments from video {video_id} to database")

if __name__ == "__main__":
    main()
