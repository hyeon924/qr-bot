import os
import sqlite3
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.errors import SlackApiError

load_dotenv()

BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
APP_TOKEN = os.getenv("SLACK_APP_TOKEN")
OPERATOR_RULES_RAW = os.getenv("OPERATOR_RULES", "")

app = App(token=BOT_TOKEN)

DB_PATH = "qr_map.db"


def validate_env():
    missing = []

    if not BOT_TOKEN:
        missing.append("SLACK_BOT_TOKEN")
    if not APP_TOKEN:
        missing.append("SLACK_APP_TOKEN")
    if not OPERATOR_RULES_RAW:
        missing.append("OPERATOR_RULES")

    if missing:
        raise ValueError(f"필수 환경변수가 누락되었습니다: {', '.join(missing)}")


def parse_operator_rules(raw: str):
    """
    형식:
    OPERATOR_RULES=운영자ID|채널ID|파일경로,운영자ID|채널ID|파일경로

    예:
    U090Q6F5K7D|C0AHLE8MU84|qr6.png,U09UC8Q7UAD|C09VDQ7DYSF|qr5.png
    """
    rules = []

    if not raw.strip():
        return rules

    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue

        parts = [p.strip() for p in item.split("|")]
        if len(parts) != 3:
            raise ValueError(
                f"OPERATOR_RULES 형식이 잘못되었습니다: '{item}' "
                f"(형식: 운영자ID|채널ID|파일경로)"
            )

        operator_user_id, target_channel_id, qr_file_path = parts

        rules.append({
            "operator_user_id": operator_user_id,
            "target_channel_id": target_channel_id,
            "qr_file_path": qr_file_path,
        })

    return rules


OPERATOR_RULES = parse_operator_rules(OPERATOR_RULES_RAW)


def find_rule(operator_user_id: str, target_channel_id: str):
    """
    운영자ID + 채널ID 조합에 맞는 규칙 찾기
    """
    for rule in OPERATOR_RULES:
        if (
            rule["operator_user_id"] == operator_user_id
            and rule["target_channel_id"] == target_channel_id
        ):
            return rule
    return None


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS qr_sent (
            key TEXT PRIMARY KEY,
            target_user TEXT NOT NULL,
            dm_channel TEXT NOT NULL,
            file_id TEXT NOT NULL,
            operator_user_id TEXT NOT NULL,
            target_channel_id TEXT NOT NULL,
            qr_file_path TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def save_mapping(
    key: str,
    target_user: str,
    dm_channel: str,
    file_id: str,
    operator_user_id: str,
    target_channel_id: str,
    qr_file_path: str
):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO qr_sent(
            key, target_user, dm_channel, file_id,
            operator_user_id, target_channel_id, qr_file_path, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
    """, (
        key, target_user, dm_channel, file_id,
        operator_user_id, target_channel_id, qr_file_path
    ))
    conn.commit()
    conn.close()


def load_mapping(key: str):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT target_user, dm_channel, file_id,
               operator_user_id, target_channel_id, qr_file_path
        FROM qr_sent
        WHERE key=?
    """, (key,))
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "target_user": row[0],
        "dm_channel": row[1],
        "file_id": row[2],
        "operator_user_id": row[3],
        "target_channel_id": row[4],
        "qr_file_path": row[5],
    }


def delete_mapping(key: str):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM qr_sent WHERE key=?", (key,))
    conn.commit()
    conn.close()


@app.event("reaction_added")
def handle_reaction_added(event, logger):
    operator_user_id = event.get("user")
    if not operator_user_id:
        return

    item = event.get("item") or {}
    target_channel_id = item.get("channel")
    ts = item.get("ts")

    if not target_channel_id or not ts:
        return

    reaction = event.get("reaction")
    target_user = event.get("item_user")  # 원본 메시지 작성자

    if not target_user:
        logger.warning("item_user not found. event ignored.")
        return

    # 같은 메시지 + 같은 작성자 기준 매핑 키
    key = f"{target_channel_id}|{ts}|{target_user}"

    logger.warning(
        f"[REACTION] reaction={reaction}, operator={operator_user_id}, "
        f"channel={target_channel_id}, ts={ts}, item_user={target_user}, key={key}"
    )

    try:
        # 1) 업로드: 허용된 운영자/채널 규칙을 반드시 통과해야 함
        if reaction == "eyes":
            rule = find_rule(operator_user_id, target_channel_id)
            if not rule:
                logger.warning(
                    f"[UPLOAD_RULE_NOT_FOUND] operator={operator_user_id}, "
                    f"channel={target_channel_id}"
                )
                return

            qr_file_path = rule["qr_file_path"]

            if not os.path.exists(qr_file_path):
                logger.error(f"QR file not found: {qr_file_path}")
                return

            existing = load_mapping(key)
            if existing:
                logger.warning(
                    f"mapping already exists. key={key} file_id={existing['file_id']}"
                )
                return

            dm = app.client.conversations_open(users=target_user)
            dm_channel = dm["channel"]["id"]

            up = app.client.files_upload_v2(
                channel=dm_channel,
                file=qr_file_path,
                title="QR",
                initial_comment="QR 전달드립니다."
            )

            file_obj = up.get("file") or {}
            file_id = file_obj.get("id")

            if not file_id:
                logger.error(f"upload ok but file_id missing: {up}")
                return

            save_mapping(
                key=key,
                target_user=target_user,
                dm_channel=dm_channel,
                file_id=file_id,
                operator_user_id=operator_user_id,
                target_channel_id=target_channel_id,
                qr_file_path=qr_file_path
            )

            logger.warning(
                f"QR uploaded and saved. "
                f"operator={operator_user_id} channel={target_channel_id} "
                f"key={key} file_id={file_id} file={qr_file_path}"
            )
            return

        # 2) 삭제: DB에 저장된 '업로드한 운영자'와 현재 운영자가 같아야만 가능
        if reaction == "완료-1":
            mapping = load_mapping(key)
            if not mapping:
                logger.warning(f"[MAPPING_NOT_FOUND] key={key} (nothing to delete)")
                return

            if mapping["operator_user_id"] != operator_user_id:
                logger.warning(
                    f"[DELETE_DENIED] uploader={mapping['operator_user_id']} "
                    f"reactor={operator_user_id} key={key}"
                )
                return

            file_id = mapping["file_id"]
            app.client.files_delete(file=file_id)
            delete_mapping(key)

            logger.warning(
                f"QR file deleted. "
                f"operator={operator_user_id} channel={target_channel_id} "
                f"key={key} file_id={file_id}"
            )
            return

    except SlackApiError as e:
        logger.error(f"SlackApiError: {e.response.data}")
    except Exception as e:
        logger.exception(e)


if __name__ == "__main__":
    validate_env()
    init_db()
    print("Starting Socket Mode...")
    SocketModeHandler(app, APP_TOKEN).start()