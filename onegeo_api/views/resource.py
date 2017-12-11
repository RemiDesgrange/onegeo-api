from django.conf import settings
from django.core.exceptions import PermissionDenied
from django.http import Http404
from django.http import JsonResponse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.views.generic import View

from onegeo_api.exceptions import ExceptionsHandler
from onegeo_api.models import Resource
from onegeo_api.models import Task
from onegeo_api.utils import BasicAuth
from onegeo_api.utils import on_http403
from onegeo_api.utils import on_http404

__all__ = ["ResourceView", "ResourceIDView"]


PDF_BASE_DIR = settings.PDF_DATA_BASE_DIR
MSG_404 = {"GetResource": {"error": "Aucune resource ne correspond à cette requête."}}


@method_decorator(csrf_exempt, name="dispatch")
class ResourceView(View):

    @BasicAuth()
    def get(self, request, src_uuid):
        user = request.user

        try:
            tsk = Task.objects.get(model_type_id=src_uuid, model_type="source")
        except Task.DoesNotExist:
            data = Resource.list_renderer(src_uuid, user=user)
            opts = {"safe": False}
        else:
            if tsk.stop_date and tsk.success is True:
                data = Resource.list_renderer(src_uuid, user=user)
                opts = {"safe": False}

            if tsk.stop_date and tsk.success is False:
                data = {"error": tsk.description,
                        "task": "tasks/{}".format(tsk.id)}
                opts = {"status": 424}

            if not tsk.stop_date and not tsk.success:
                data = {"error": tsk.description,
                        "task": "tasks/{}".format(tsk.id)}
                opts = {"status": 423}

        return JsonResponse(data, **opts)


@method_decorator(csrf_exempt, name="dispatch")
class ResourceIDView(View):

    @BasicAuth()
    @ExceptionsHandler(actions={Http404: on_http404, PermissionDenied: on_http403}, model="Resource")
    def get(self, request, src_uuid, rsrc_uuid):
        resource = Resource.get_with_permission(rsrc_uuid, request.user)
        return JsonResponse(resource.detail_renderer, safe=False)
