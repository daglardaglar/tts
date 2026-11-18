import asyncio
import json
import os
import uuid
from dataclasses import dataclass, field
import websockets
from websockets.asyncio.server import ServerConnection


@dataclass
class Room:
    clients: dict[ServerConnection, str] = field(default_factory=dict)
    queue: list[dict] = field(default_factory=list)
    active_msg_id: str | None = None


ROOMS: dict[str, Room] = {}


async def broadcast(room: Room, payload: dict) -> None:
    if not room.clients:
        return
    data = json.dumps(payload, ensure_ascii=False)
    await asyncio.gather(
        *(ws.send(data) for ws in list(room.clients.keys())),
        return_exceptions=True,
    )


async def start_next(room: Room) -> None:
    if room.active_msg_id is not None or not room.queue:
        return
    next_item = room.queue.pop(0)
    room.active_msg_id = next_item["msg_id"]
    await broadcast(
        room,
        {
            "type": "speak",
            "msg_id": next_item["msg_id"],
            "nick": next_item["nick"],
            "text": next_item["text"],
        },
    )


async def handler(ws: ServerConnection) -> None:
    room_id: str | None = None
    nick = "Misafir"

    try:
        async for raw in ws:
            msg = json.loads(raw)
            msg_type = msg.get("type")

            if msg_type == "join":
                room_id = str(msg.get("room", "genel")).strip() or "genel"
                nick = str(msg.get("nick", "Misafir")).strip() or "Misafir"
                room = ROOMS.setdefault(room_id, Room())
                room.clients[ws] = nick
                await broadcast(room, {"type": "system", "text": f"{nick} odaya katıldı."})

            elif msg_type == "chat" and room_id in ROOMS:
                text = str(msg.get("text", "")).strip()
                if not text:
                    continue
                room = ROOMS[room_id]
                msg_id = uuid.uuid4().hex
                await broadcast(room, {"type": "chat", "nick": nick, "text": text})
                room.queue.append({"msg_id": msg_id, "nick": nick, "text": text, "ws": ws})
                await start_next(room)

            elif msg_type == "done" and room_id in ROOMS:
                room = ROOMS[room_id]
                if msg.get("msg_id") == room.active_msg_id:
                    room.active_msg_id = None
                    await start_next(room)

    except websockets.ConnectionClosed:
        pass
    finally:
        if room_id and room_id in ROOMS:
            room = ROOMS[room_id]
            left_nick = room.clients.pop(ws, nick)
            # Çıkan kişinin bekleyen mesajlarını temizle, aktifse sırayı ilerlet
            room.queue = [item for item in room.queue if item["ws"] is not ws]
            if not room.clients:
                ROOMS.pop(room_id, None)
            else:
                await broadcast(room, {"type": "system", "text": f"{left_nick} ayrıldı."})


async def main() -> None:
    host = "0.0.0.0"
    port = int(os.environ.get("PORT", 8765))
    print(f"WebSocket Oda Sunucusu çalışıyor: ws://{host}:{port}")
    async with websockets.serve(handler, host, port):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
