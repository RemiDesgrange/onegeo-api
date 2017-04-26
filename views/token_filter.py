import json
from django.conf import settings
from django.http import HttpResponse, JsonResponse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.views.generic import View

from .. import utils
from ..exceptions import JsonError
from ..models import Filter


__all__ = ["TokenFilterView", "TokenFilterIDView"]


PDF_BASE_DIR = settings.PDF_DATA_BASE_DIR
MSG_406 = "Le format demandé n'est pas pris en charge. "


@method_decorator(csrf_exempt, name="dispatch")
class TokenFilterView(View):

    def get(self, request):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        return JsonResponse(utils.get_objects(user(), Filter), safe=False)

    def post(self, request):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user

        if "application/json" not in request.content_type:
            return JsonResponse([{"error": "Content-type incorrect"}], safe=False)
        data = request.body.decode('utf-8')
        body_data = json.loads(data)

        name = utils.read_name(body_data)
        if name is None:
            return JsonResponse({"error": "Echec de création du filtre. Le nom du filtre est manquant. "}, status=400)
        if Filter.objects.filter(name=name).count() > 0:
            return JsonResponse({"error": "Echec de création du filtre. Un filtre portant le même nom existe déjà. "}, status=409)

        cfg = "config" in body_data and body_data["config"] or {}

        filter, created = Filter.objects.get_or_create(name=name, defaults={"config":cfg,
                                                                           "user":user()})
        status = created and 201 or 409
        return utils.format_json_get_create(request, created, status, filter.name)


@method_decorator(csrf_exempt, name="dispatch")
class TokenFilterIDView(View):

    def get(self, request, name):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        name = (name.endswith('/') and name[:-1] or name)
        try:
            utils.user_access(name, Filter, user())
        except JsonError as e:
            return JsonResponse(data={"error": e.message}, status=e.status)

        return JsonResponse(utils.get_object_id(user(), name, Filter))

    def put(self, request, name):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user

        if "application/json" not in request.content_type:
            return JsonResponse({"error": MSG_406}, status=406)
        data = request.body.decode('utf-8')
        body_data = json.loads(data)

        cfg = "config" in body_data and body_data["config"] or {}

        flt_name = (name.endswith('/') and name[:-1] or name)
        filter = Filter.objects.filter(name=flt_name, user=user())

        if len(filter) == 1:
            filter.update(config=cfg)
            status = 204
            data = {}
        elif len(filter) == 0:
            flt = Filter.objects.filter(name=flt_name)
            if len(flt) == 1:
                status = 403
                data = {"error": "Echec de mise à jour du filtre. Vous ne disposez pas des autorisations requises. "}

        return JsonResponse(data, status=status)

    def delete(self, request, name):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        name = (name.endswith('/') and name[:-1] or name)
        return utils.delete_func(name, user(), Filter)
