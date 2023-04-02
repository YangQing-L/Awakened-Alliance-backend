from channels.generic.websocket import AsyncWebsocketConsumer
import json
from django.conf import settings
from django.core.cache import cache

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from MatchSystem.src.match_server.match_service import Match
from AwakenedAlliance.models.player.player import Player
from channels.db import database_sync_to_async


class MultiPlayer(AsyncWebsocketConsumer):
    # 前端创建连接的时候调用
    async def connect(self):
        await self.accept()


    # 前端刷新或者close()时会执行，但不一定（比如电脑突然断电）
    async def disconnect(self, close_code):
        if self.room_name:
            await self.channel_layer.group_discard(self.room_name, self.channel_name)


    # 创建新的玩家时分组，并群发消息
    async def create_player(self, data):
        self.room_name = None
        self.uuid = data['uuid']
        # Make socket
        transport = TSocket.TSocket('127.0.0.1', 9090)

        # Buffering is critical. Raw sockets are very slow
        transport = TTransport.TBufferedTransport(transport)

        # Wrap in a protocol
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        # Create a client to use the protocol encoder
        client = Match.Client(protocol)

        def db_get_player():
            return Player.objects.get(user__username=data['username'])

        player = await database_sync_to_async(db_get_player)()

        # Connect!
        transport.open()

        client.add_player(player.rank_score, data['uuid'], data['username'], data['photo'], self.channel_name)

        # Close!
        transport.close()

        # 之前的代码
        # self.room_name = None
        
        # for i in range(1000):
        #     name = "room-%d" %(i)
        #     if not cache.has_key(name) or len(cache.get(name)) < settings.ROOM_CAPACITY:
        #         self.room_name = name
        #         break
        
        # if not self.room_name:
        #     print("[MY ERROR] do not have enought room name")
        #     return
        
        # if not cache.has_key(self.room_name):
        #     cache.set(self.room_name, [], 3600)  # 对战有效期一个小时
        
        # # 建立连接之后服务器向本地发送当前所有的玩家信息
        # for player in cache.get(self.room_name):
        #     # json.dumps：将一个json数据结构转换成字符串
        #     await self.send(text_data=json.dumps({
        #         'event': "create_player",
        #         'uuid': player['uuid'],
        #         'username': player['username'],
        #         'photo': player['photo'],
        #     }))

        # await self.channel_layer.group_add(self.room_name, self.channel_name)

        # players = cache.get(self.room_name)
        # players.append({
        #     'uuid': data['uuid'],
        #     'username': data['username'],
        #     'photo': data['photo'],
        # })
        # cache.set(self.room_name, players, 3600)
        # # 群发消息
        # await self.channel_layer.group_send(
        #     self.room_name,
        #     {
        #         'type': "group_send_event",
        #         'event': "create_player",
        #         'uuid': data['uuid'],
        #         'username': data['username'],
        #         'photo': data['photo'],
        #     }
        # )


    async def move_toward(self, data):
        await self.channel_layer.group_send(
            self.room_name,
            {
                'type': "group_send_event",
                'event': "move_toward",
                'uuid': data['uuid'],
                'directions_array': data['directions_array'],
                'x': data['x'],
                'y': data['y'],
            }
        )


    async def use_general_skill(self, data):
        await self.channel_layer.group_send(
            self.room_name,
            {
                'type': "group_send_event",
                'event': "use_general_skill",
                'uuid': data['uuid'],
                'skill_data': data['skill_data'],
            }
        )


    async def use_summoner_skill(self, data):
        await self.channel_layer.group_send(
            self.room_name,
            {
                'type': "group_send_event",
                'event': "use_summoner_skill",
                'uuid': data['uuid'],
                'skill_data': data['skill_data'],
            }
        )


    async def attack(self, data):
        if not self.room_name:
            return 
        players = cache.get(self.room_name)

        if not players:
            return
        
        for player in players:
            if player['uuid'] == data['attackee_uuid']:
                player['hp'] -= data['damage']

        remain_cnt = 0
        for player in players:
            if player['hp'] > 0:
                remain_cnt += 1
        if remain_cnt > 1:
            if self.room_name:
                cache.set(self.room_name, players, 3600)
        else:
            def db_update_player_rank_score(username, rank_score):
                player = Player.objects.get(user__username=username)
                player.rank_score += rank_score
                player.save()
            for player in players:
                if player['hp'] <= 0:
                    await database_sync_to_async(db_update_player_rank_score)(player['username'], -5)
                else:
                    await database_sync_to_async(db_update_player_rank_score)(player['username'], +10)
        
        await self.channel_layer.group_send(
            self.room_name,
            {
                'type': "group_send_event",
                'event': "attack",
                'uuid': data['uuid'],
                'attackee_uuid': data['attackee_uuid'],
                'x': data['x'],
                'y': data['y'],
                'angle': data['angle'],
                'damage': data['damage'],
                'ball_uuid': data['ball_uuid'],                
            }
        )


    async def message(self, data):
        await self.channel_layer.group_send(
            self.room_name,
            {
                'type': "group_send_event",
                'event': "message",
                'uuid': data['uuid'],
                'username': data['username'],
                'text': data['text'],
            }
        )


    # 向当前连接的前端发送消息，函数名与'type'关键字保持一致
    # 所有的事件都可以通过调用group_send_event()来实现
    async def group_send_event(self, data):
        # 匹配进程调用的时候可能还没生成room_name，要在这里获取一下
        if not self.room_name:
            keys = cache.keys('*%s*' % (self.uuid))
            if keys:
                self.room_name = keys[0]
        await self.send(text_data=json.dumps(data))


    # 接收前端向后端发送的请求
    async def receive(self, text_data):
        data = json.loads(text_data)
        event = data['event']
        if event == "create_player":
            await self.create_player(data)
        elif event == "move_toward":
            await self.move_toward(data)
        elif event == "general_skill":
            await self.use_general_skill(data)
        elif event == "summoner_skill":
            await self.use_summoner_skill(data)
        elif event == "attack":
            await self.attack(data)
        elif event == "message":
            await self.message(data)
