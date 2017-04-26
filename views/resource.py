from ast import literal_eval
from django.conf import settings
from django.http import HttpResponse, JsonResponse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.views.generic import View

from .. import utils
from ..models import Resource, Task


__all__ = ["ResourceView", "ResourceIDView"]


PDF_BASE_DIR = settings.PDF_DATA_BASE_DIR
MSG_406 = "Le format demandé n'est pas pris en charge. "


@method_decorator(csrf_exempt, name="dispatch")
class ResourceView(View):

    def get(self, request, id):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        src_id = literal_eval(id)

        try:
            tsk = Task.objects.get(model_type_id=src_id, model_type="source")
        except Task.DoesNotExist:
            data = utils.get_objects(user(), Resource, src_id)
            opts = {"safe": False}
        else:
            if tsk.stop_date is not None and tsk.success is True:
                data = utils.get_objects(user(), Resource, src_id)
                opts = {"safe": False}

            if tsk.stop_date is not None and tsk.success is False:
                data = {"error": tsk.description,
                        "task": "tasks/{}".format(tsk.id)}
                opts = {"status": 424}

            if tsk.stop_date is None and tsk.success is None:
                data = {"error": tsk.description,
                        "task": "tasks/{}".format(tsk.id)}
                opts = {"status": 423}

        return JsonResponse(data, **opts)


@method_decorator(csrf_exempt, name="dispatch")
class ResourceIDView(View):

    def get(self, request, src_id, rsrc_id):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        return JsonResponse(utils.get_object_id(user(), rsrc_id, Resource, src_id), safe=False)

