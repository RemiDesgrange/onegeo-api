from django.conf import settings
from django.http import HttpResponse, JsonResponse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.views.generic import View
from pathlib import Path

from .. import utils
from ..models import Source


__all__ = ["Directories", "SupportedModes"]


PDF_BASE_DIR = settings.PDF_DATA_BASE_DIR


def file_uri_shortcut(b):
    """
    Retourne une liste d'uri sous la forme de "file:///dossier",
    afin de caché le chemin absolue des sources
    """
    p = Path(b.startswith("file://") and b[7:] or b)
    if not p.exists():
        raise ConnectionError("Given path does not exist.")
    l = []
    for x in p.iterdir():
        if x.is_dir():
            l.append("file:///{}".format(x.name))
    return l


@method_decorator(csrf_exempt, name="dispatch")
class Directories(View):

    def get(self, request):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        subdir = file_uri_shortcut(PDF_BASE_DIR)
        return JsonResponse(subdir, safe=False)


@method_decorator(csrf_exempt, name="dispatch")
class SupportedModes(View):

    def get(self, request):
        return JsonResponse(dict((m[0], m[1]) for m in Source.MODE_L), safe=False)
