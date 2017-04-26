import json
from django.conf import settings
from django.http import HttpResponse, JsonResponse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.views.generic import View

from .. import utils
from ..exceptions import JsonError
from ..models import Tokenizer


__all__ = ["TokenizerView", "TokenizerIDView"]


PDF_BASE_DIR = settings.PDF_DATA_BASE_DIR
MSG_406 = "Le format demandé n'est pas pris en charge. "


@method_decorator(csrf_exempt, name="dispatch")
class TokenizerView(View):

    def get(self, request):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        return JsonResponse(utils.get_objects(user(), Tokenizer), safe=False)

    def post(self, request):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user

        if "application/json" not in request.content_type:
            return JsonResponse({"Error": MSG_406}, status=406)
        data = request.body.decode('utf-8')
        body_data = json.loads(data)

        name = utils.read_name(body_data)
        if name is None:
            return JsonResponse({"error": "Echec de création du tokenizer. Le nom du tokenizer est manquant. "}, status=400)
        if Tokenizer.objects.filter(name=name).count() > 0:
            return JsonResponse({"error": "Echec de création du tokenizer. Un tokenizer portant le même nom existe déjà. "}, status=409)

        cfg = "config" in body_data and body_data["config"] or {}

        token, created = Tokenizer.objects.get_or_create(config=cfg, user=user(), name=name)
        status = created and 201 or 409
        return utils.format_json_get_create(request, created, status, token.name)


@method_decorator(csrf_exempt, name="dispatch")
class TokenizerIDView(View):

    def get(self, request, name):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        name = (name.endswith('/') and name[:-1] or name)

        try:
            utils.user_access(name, Tokenizer, user())
        except JsonError as e:
            return JsonResponse(data={"error": e.message}, status=e.status)

        return JsonResponse(utils.get_object_id(user(), name, Tokenizer), safe=False)

    def put(self, request, name):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user

        if "application/json" not in request.content_type:
            return JsonResponse({"Error": MSG_406}, status=406)
        data = request.body.decode('utf-8')
        body_data = json.loads(data)

        cfg = "config" in body_data and body_data["config"] or {}

        name = (name.endswith('/') and name[:-1] or name)

        try:
            utils.user_access(name, Tokenizer, user())
        except JsonError as e:
            return JsonResponse(data={"error": e.message}, status=e.status)

        Tokenizer.objects.filter(name=name).update(config=cfg)

        return JsonResponse(data={}, status=204)

    def delete(self, request, name):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        name = (name.endswith('/') and name[:-1] or name)

        return utils.delete_func(name, user(), Tokenizer)
