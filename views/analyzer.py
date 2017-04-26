import json
from django.conf import settings
from django.db import transaction
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.views.generic import View

from .. import utils
from ..exceptions import JsonError
from ..models import Filter, Analyzer, Tokenizer


__all__ = ["AnalyzerView", "AnalyzerIDView"]


PDF_BASE_DIR = settings.PDF_DATA_BASE_DIR
MSG_406 = "Le format demandé n'est pas pris en charge. "


@method_decorator(csrf_exempt, name="dispatch")
class AnalyzerView(View):

    def get(self, request):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        return JsonResponse(utils.get_objects(user(), Analyzer), safe=False)

    @transaction.atomic
    def post(self, request):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user

        if "application/json" not in request.content_type:
            return JsonResponse({"error": MSG_406}, status=406)
        data = request.body.decode('utf-8')
        body_data = json.loads(data)
        name = utils.read_name(body_data)
        if name is None:
            return JsonResponse({"error": "Echec de création de l'analyseur. Le nom de l'analyseur est manquant."}, status=400)
        if Analyzer.objects.filter(name=name).count() > 0:
            return JsonResponse({"error": "Echec de la création de l'analyseur. Un analyseur portant le même nom existe déjà. "}, status=409)

        tokenizer = "tokenizer" in body_data and body_data["tokenizer"] or None
        filters = "filters" in body_data and body_data["filters"] or []

        analyzer, created = Analyzer.objects.get_or_create(user=user(), name=name)
        if created and len(filters) > 0:
            for f in filters:
                try:
                    flt = Filter.objects.get(name=f)
                    analyzer.filter.add(flt)
                    analyzer.save()
                except Filter.DoesNotExist:
                    return JsonResponse({"error": "Echec de création de l'analyseur. La liste contient un ou plusieurs filtres n'existant pas. "}, status=400)

        if created and tokenizer is not None:
            try:
                tkn_chk = Tokenizer.objects.get(name=tokenizer)
                analyzer.tokenizer = tkn_chk
                analyzer.save()
            except Tokenizer.DoesNotExist:
                return JsonResponse({"error": "Echec de création de l'analyseur: Le tokenizer n'existe pas. "}, status=400)
        status = created and 201 or 409
        return utils.format_json_get_create(request, created, status, analyzer.name)


@method_decorator(csrf_exempt, name="dispatch")
class AnalyzerIDView(View):

    def get(self, request, name):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        try:
            utils.user_access(name, Analyzer, user())
        except JsonError as e:
            return JsonResponse(data={"error": e.message}, status=e.status)

        return JsonResponse(utils.get_object_id(user(), name, Analyzer))

    def put(self, request, name):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user

        if "application/json" not in request.content_type:
            return JsonResponse({"error": MSG_406}, status=406)
        data = request.body.decode('utf-8')
        body_data = json.loads(data)

        tokenizer = "tokenizer" in body_data and body_data["tokenizer"] or False
        filters = "filters" in body_data and body_data["filters"] or []

        name = (name.endswith('/') and name[:-1] or name)
        analyzer = get_object_or_404(Analyzer, name=name)

        if tokenizer:
            try:
                tkn_chk = Tokenizer.objects.get(name=tokenizer)
            except Tokenizer.DoesNotExist:
                return JsonResponse({"error": "Echec de mise à jour du tokenizer. "
                                              "Le tokenizer n'existe pas. "}, status=400)

        if analyzer.user != user():
            status = 403
            data = {"error": "Forbidden"}
        else:
            status = 204
            data = {}
            # On s'assure que tous les filtres existent
            for f in filters:
                try:
                    flt = Filter.objects.get(name=f)
                except Filter.DoesNotExist:
                    return JsonResponse({"error": "Echec de mise à jour du tokenizer. "
                                                  "La liste contient un ou plusieurs "
                                                  "filtres n'existant pas. "}, status=400)
            # Si tous corrects, on met à jour depuis un set vide
            analyzer.filter.set([])
            for f in filters:
                analyzer.filter.add(f)
            if tokenizer:
                analyzer.tokenizer = tkn_chk
                analyzer.save()

        return JsonResponse(data, status=status)

    def delete(self, request, name):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        name = (name.endswith('/') and name[:-1] or name)

        return utils.delete_func(name, user(), Analyzer)

