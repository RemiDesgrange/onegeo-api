import json
from ast import literal_eval
from django.conf import settings
from django.core.exceptions import ValidationError
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.views.generic import View
from re import search
from onegeo_manager.context import Context as OnegeoContext
from onegeo_manager.index import Index as OnegeoIndex
from onegeo_manager.resource import Resource as OnegeoResource
from onegeo_manager.source import Source as OnegeoSource

from .. import utils
from ..models import Context, Resource, Source, Task


__all__ = ["ContextView", "ContextIDView",
           "ContextIDTaskView", "ContextIDTaskIDView"]


PDF_BASE_DIR = settings.PDF_DATA_BASE_DIR
MSG_406 = "Le format demandé n'est pas pris en charge. "


def check_columns(list_ppt, list_ppt_clt):
    for ppt in list_ppt:
        for ppt_clt in list_ppt_clt:
            if ppt["name"] == ppt_clt["name"]:
                ppt.update(ppt_clt)
    return list_ppt


@method_decorator(csrf_exempt, name="dispatch")
class ContextView(View):

    def get(self, request):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        return JsonResponse(utils.get_objects(user(), Context), safe=False)

    def post(self, request):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user

        if "application/json" not in request.content_type:
            return JsonResponse({"error": MSG_406}, status=406)

        body_data = json.loads(request.body.decode('utf-8'))
        if "name" not in body_data:
            return JsonResponse({"error": "Echec la création du contexte d'indexation. "
                                          "Le nom du contexte est manquant. "}, status=400)
        if "resource" not in body_data:
            return JsonResponse({"error": "Echec de création du contexte d'indexation. "
                                          "Le chemin d'accès est manquant. "}, status=400)

        name = utils.read_name(body_data)
        if name is None:
            return JsonResponse({"error": "Echec de création du contexte d'indexation. "
                                          "Le nom du context est incorrect. "}, status=400)
        if Context.objects.filter(name=name).count() > 0:
            return JsonResponse({"error": "Echec de création du contexte d'indexation. "
                                          "Un contexte portant le même nom existe déjà. "}, status=409)

        reindex_frequency = "monthly"
        if "reindex_frequency" in body_data:
            reindex_frequency = body_data['reindex_frequency']

        data = search('^/sources/(\d+)/resources/(\d+)$', body_data['resource'])
        if not data:
            return None
        src_id = data.group(1)
        rsrc_id = data.group(2)
        set_src = get_object_or_404(Source, id=src_id)

        set_rscr = get_object_or_404(Resource, source=set_src, id=rsrc_id)
        if Context.objects.filter(resource__id=rsrc_id).count() > 0:
            return JsonResponse({"error": "Echec de création du contexte d'indexation. "
                                          "Une ressource ne peut être liée à plusieurs "
                                          "contextes d'indexation. "}, status=409)

        onegeo_source = OnegeoSource(set_src.uri, name, set_src.mode)
        onegeo_resource = OnegeoResource(onegeo_source, set_rscr.name)
        for col in iter(set_rscr.columns):
            if onegeo_resource.is_existing_column(col["name"]):
                continue
            onegeo_resource.add_column(
                            col["name"], column_type=col["type"],
                            occurs=tuple(col["occurs"]), count=col["count"])

        onegeo_index = OnegeoIndex(set_rscr.name)
        onegeo_context = OnegeoContext(name, onegeo_index, onegeo_resource)
        column_ppt = []
        for property in onegeo_context.iter_properties():
            column_ppt.append(property.all())

        try:
            context = Context.objects.create(resource=set_rscr,
                                             name=name,
                                             clmn_properties=column_ppt,
                                             reindex_frequency=reindex_frequency)
        except ValidationError as e:
            return JsonResponse(data={"error":e.message}, status=409)

        response = JsonResponse(data={}, status=201)
        response['Location'] = '{}{}'.format(request.build_absolute_uri(), context.resource_id)
        return response


@method_decorator(csrf_exempt, name="dispatch")
class ContextIDView(View):

    def get(self, request, id):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        ctx_id = literal_eval(id)

        return JsonResponse(utils.get_object_id(user(), ctx_id, Context),
                            safe=False, status=200)

    def put(self, request, id):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user

        if "application/json" not in request.content_type:
            return JsonResponse([{"error": MSG_406}], safe=False)
        data = request.body.decode('utf-8')
        body_data = json.loads(data)


        if "name" in body_data:
            name = body_data['name']

        reindex_frequency = None
        if "reindex_frequency" in body_data:
            reindex_frequency = body_data['reindex_frequency']

        list_ppt_clt = {}
        if "columns" in body_data:
            list_ppt_clt = body_data['columns']

        data = search('^/sources/(\d+)/resources/(\d+)$', body_data['resource'])
        if not data:
            return None
        src_id = data.group(1)
        rsrc_id = data.group(2)
        set_src = get_object_or_404(Source, id=src_id)
        set_rscr = get_object_or_404(Resource, source=set_src, id=rsrc_id)

        ctx_id = literal_eval(id)
        context = get_object_or_404(Context, resource_id=ctx_id)

        list_ppt = context.clmn_properties
        ppt_update = check_columns(list_ppt, list_ppt_clt)

        context.resource = set_rscr
        context.name = name
        context.clmn_properties = ppt_update
        if reindex_frequency:
            context.reindex_frequency = reindex_frequency
        context.save()

        return JsonResponse(data={}, status=204)

    def delete(self, request, id):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user

        id = literal_eval(id)

        return utils.delete_func(id, user(), Context)


@method_decorator(csrf_exempt, name="dispatch")
class ContextIDTaskView(View):

    def get(self, request, id):

        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        ctx_id = literal_eval(id)

        get_object_or_404(Context, pk=ctx_id)

        set = Task.objects.filter(model_type="context",
                                  model_type_id=ctx_id,
                                  user=user()).order_by("-start_date")

        data = [utils.format_task(tsk) for tsk in set]

        return JsonResponse(data, safe=False)


@method_decorator(csrf_exempt, name="dispatch")
class ContextIDTaskIDView(View):

    def get(self, request, ctx_id, tsk_id):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user

        tsk_id = literal_eval(tsk_id)
        tsk = get_object_or_404(Task, pk=tsk_id, model_type_id=ctx_id)

        return JsonResponse(utils.format_task(tsk), safe=False)

