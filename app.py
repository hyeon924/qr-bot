import os
import sqlite3
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.errors import SlackApiError

load_dotenv()

BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
APP_TOKEN = os.getenv("SLACK_APP_TOKEN")
OPERATOR_USER_ID = os.getenv("OPERATOR_USER_ID")
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID")
QR_FILE_PATH = os.getenv("QR_FILE_PATH", "qr.png")

app = App(token=BOT_TOKEN)

DB_PATH = "qr_map.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS qr_sent (
            key TEXT PRIMARY KEY,
            target_user TEXT NOT NULL,
            dm_channel TEXT NOT NULL,
            file_id TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

def save_mapping(key: str, target_user: str, dm_channel: str, file_id: str):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO qr_sent(key, target_user, dm_channel, file_id, created_at)
        VALUES (?, ?, ?, ?, datetime('now'))
    """, (key, target_user, dm_channel, file_id))
    conn.commit()
    conn.close()

def load_mapping(key: str):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT target_user, dm_channel, file_id FROM qr_sent WHERE key=?", (key,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return {"target_user": row[0], "dm_channel": row[1], "file_id": row[2]}

def delete_mapping(key: str):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM qr_sent WHERE key=?", (key,))
    conn.commit()
    conn.close()

@app.event("reaction_added")
def handle_reaction_added(event, logger):
    # 운영자만
    if event.get("user") != OPERATOR_USER_ID:
        return

    item = event.get("item") or {}
    ch = item.get("channel")
    ts = item.get("ts")
    if ch != TARGET_CHANNEL_ID or not ts:
        return

    reaction = event.get("reaction")
    target_user = event.get("item_user")  # 메시지 작성자 (history 불필요)

    # 매칭 키(원본 메시지 기준)
    key = f"{ch}|{ts}|{target_user}"

    try:
        # 1) eyes: QR 업로드 + 저장
        if reaction == "eyes":
            dm = app.client.conversations_open(users=target_user)
            dm_channel = dm["channel"]["id"]

            # Slack에 파일 업로드(링크 노출 X)
            up = app.client.files_upload_v2(
                channel=dm_channel,
                file=QR_FILE_PATH,
                title="QR",
                initial_comment="QR 전달드립니다."
            )

            # files_upload_v2 응답 구조에서 file id 추출
            file_obj = up.get("file") or {}
            file_id = file_obj.get("id")
            if not file_id:
                logger.error(f"upload ok but file_id missing: {up}")
                return

            save_mapping(key, target_user, dm_channel, file_id)
            logger.warning(f"QR uploaded and saved. key={key} file_id={file_id}")
            return

        # 2) 완료-1: 업로드했던 QR 파일만 삭제
        if reaction == "완료-1":
            m = load_mapping(key)
            if not m:
                logger.warning(f"no mapping found for key={key} (nothing to delete)")
                return

            file_id = m["file_id"]
            app.client.files_delete(file=file_id)
            delete_mapping(key)
            logger.warning(f"QR file deleted. key={key} file_id={file_id}")
            return

    except SlackApiError as e:
        logger.error(f"SlackApiError: {e.response.data}")
    except Exception as e:
        logger.exception(e)

if __name__ == "__main__":
    init_db()
    print("Starting Socket Mode...")
    SocketModeHandler(app, APP_TOKEN).start()