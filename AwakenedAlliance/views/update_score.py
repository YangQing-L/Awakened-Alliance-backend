from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from AwakenedAlliance.models.player.player import Player


class UpdateScoreView(APIView):
    permission_classes = ([IsAuthenticated])

    def post(self, request):
        score = int(request.POST.get('score', 0))
        if  score < 0:
            return Response({
                'result': "score参数不合法"
            })
        player = Player.objects.get(user=request.user)
        player.score = max(player.score, score)
        player.save()
        return Response({
            'result': "success",
        })