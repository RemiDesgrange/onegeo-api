from base64 import b64decode
from django.conf import settings
from django.contrib.auth import authenticate
from django.contrib.auth.models import User
from django.core.exceptions import FieldDoesNotExist, ObjectDoesNotExist
from django.db.models import Q
from django.http import HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404
from re import search

from .models import Analyzer, Context, Filter, Resource, \
                    Source, SearchModel, Task, Tokenizer
from .exceptions import JsonError


PDF_BASE_DIR = settings.PDF_DATA_BASE_DIR


def format_source(s):
    return clean_my_obj({"id": s.id,
                         "uri": s.s_uri,
                         "mode": s.mode,
                         "name": s.name,
                         "location": "/sources/{}".format(s.id)})


def format_resource(s, r):
    d = {}
    try:
        ctx = Context.objects.get(resource_id=r.id)
        d = {"id": r.id,
             "location": "/sources/{}/resources/{}".format(s.id, r.id),
             "name": r.name,
             "columns": r.columns,
             "indices": format_context(s, r, ctx)["location"]}
    except Context.DoesNotExist:
        d = {"id": r.id,
             "location": "/sources/{}/resources/{}".format(s.id, r.id),
             "name": r.name,
             "columns": r.columns}
    finally:
        return clean_my_obj(d)


def format_context(s, r, c):
    return {"location": "/indices/{}".format(c.resource_id),
            "resource": "/sources/{}/resources/{}".format(s.id, r.id),
            "columns": c.clmn_properties,
            "name": c.name,
            "reindex_frequency": c.reindex_frequency}


def format_filter(obj):
    return clean_my_obj({"location": "tokenfilters/{}".format(obj.name),
                         "name": obj.name,
                         "config": obj.config or None,
                         "reserved": obj.reserved})


def format_tokenizer(obj):
    return clean_my_obj({"location": "tokenizers/{}".format(obj.name),
                         "name": obj.name,
                         "config": obj.config or None,
                         "reserved": obj.reserved})


def format_analyzer(obj):

    def retreive_filters(name):
        af = Analyzer.filter.through
        set = af.objects.filter(analyzer__name=name).order_by("id")
        return [s.filter.name for s in set if s.filter.name is not None]

    return clean_my_obj({
                "location": "analyzers/{}".format(obj.name),
                "name": obj.name,
                "tokenfilters": retreive_filters(obj.name) or None,
                "reserved": obj.reserved,
                "tokenizer": obj.tokenizer and obj.tokenizer.name or None})


def format_task(obj):
    return clean_my_obj({
                "id": obj.pk,
                "status": obj.success is None and 'running' or 'done',
                "description": obj.description,
                "location": "tasks/{}".format(obj.pk),
                "success": obj.success,
                "dates": {"start": obj.start_date, "stop": obj.stop_date}})


def format_search_model(obj):

    def retreive_contexts(name):
        smc = SearchModel.context.through
        set = smc.objects.filter(searchmodel__name=name)
        return [s.context.name for s in set if s.context.name is not None]

    return clean_my_obj({
                "location": "profiles/{}".format(obj.name),
                "name": obj.name,
                "config": obj.config,
                "indices": retreive_contexts(obj.name)})


def get_objects(user, mdl, src_id=None):
    # Formate la réponse Json selon le type de 'Model' pour un ensemble d'objets

    l = []
    d = {Tokenizer: format_tokenizer,
         Analyzer: format_analyzer,
         Filter: format_filter}

    if mdl in d:
        obj = mdl.objects.filter(Q(user=user) | Q(user=None)).order_by("reserved", "name")
        for o in obj:
            l.append(d[mdl](o))

    if mdl is SearchModel:
        search_model = SearchModel.objects.filter(Q(user=user) | Q(user=None)).order_by("name")
        for sm in search_model:
            l.append(format_search_model(sm))

    if mdl is Task:
        task = Task.objects.filter(Q(user=user) | Q(user=None)).order_by("-start_date")
        for tsk in task:
            l.append(format_task(tsk))

    if mdl is Context:
        src = Source.objects.filter(user=user)
        for s in src:
            rsrc = Resource.objects.filter(source=s, source__user=user)
            for r in rsrc:
                set = Context.objects.filter(resource_id=r.id, resource__source__user=user)
                for ctx in set:
                    l.append(format_context(s, r, ctx))

    if mdl is Resource and src_id is not None:
        source = Source.objects.get(id=src_id, user=user)
        rsrc = Resource.objects.filter(source=source, source__user=user).order_by("name")
        for r in rsrc:
            l.append(format_resource(source, r))

    if mdl is Source:
        src = Source.objects.filter(user=user).order_by("name")
        for s in src:
            l.append(format_source(s))

    return l


def get_object_id(user, id, mdl, mdl_id=None):
    # Formate la réponse Json selon le type de model pour un objet identifié

    l = {}
    d = {SearchModel : format_search_model,
         Tokenizer: format_tokenizer,
         Analyzer : format_analyzer,
         Filter : format_filter}

    if mdl in d:
        obj = get_object_or_404(mdl, name=id)
        if obj.user == user or obj.user is None:
            l = d[mdl](obj)

    if mdl is Context and mdl_id is None:
        src = Source.objects.filter(user=user)
        for s in src:
            rsrc = Resource.objects.filter(source=s, source__user=user)
            for r in rsrc:
                if r.id == id:
                    ctx = Context.objects.get(resource_id=r.id, resource__source__user=user)
                    l = format_context(s, r, ctx)

    if mdl is Resource and mdl_id is not None:
        source = get_object_or_404(Source, id=mdl_id, user=user)
        rsrc = get_object_or_404(Resource, id=id, source=source, source__user=user)
        l = format_resource(source, rsrc)

    if mdl is Source:
        source = get_object_or_404(Source, id=id, user=user)
        l = format_source(source)

    if mdl is Task:
        task = get_object_or_404(Task, id=id, user=user)
        l = format_task(task)

    return l


def read_name(body_data):
    if "name" not in body_data or body_data["name"] == "":
        return None
    try:
        name = search("^[a-z0-9_]{2,100}$", body_data["name"])
        name = name.group(0)
    except AttributeError:
        return None
    return name


class UserAuthenticate:
    """USAGE views.py/functions:
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        my_user= user()

        DEPLOIMENT APACHE/WSGI:
        -----------------------
        fichier conf apache2 : 'WSGIPassAuthorization ON'
        ------------------------------------------------
    """

    # Authentification clé api + droits liés

    def __init__(self, request):
        self.request = request

    def __enter__(self):
        if "HTTP_AUTHORIZATION" in self.request.META:
            auth = self.request.META["HTTP_AUTHORIZATION"].split()
            if len(auth) == 2:
                if auth[0].lower() == "basic":
                    uname, passwd = b64decode(auth[1]).decode("utf-8").split(":")
                    user = authenticate(username=uname, password=passwd)
                    if user:
                        return User.objects.get(username=uname)
                    else:
                        return None

    def __call__(self, *args, **kwargs):
        with UserAuthenticate(self.request) as user:
            if not user:
                return None
            return user

    def __exit__(self, *args):
        pass


def get_user_or_401(request):

    user = UserAuthenticate(request)
    if user() is None:
        response = HttpResponse()
        response.status_code = 401
        response["WWW-Authenticate"] = 'Basic realm="%s"' % "Basic Auth Protected"
        return response

    return user



def format_json_get_create(request, created, status, obj_id):
    # Format response Json apres les get_or_create()

    if created:
        response = JsonResponse(data={}, status=status)
        response["Location"] = "{}{}".format(request.build_absolute_uri(), obj_id)
    if created is False:
        data = {"error": "Echec de la création: L'élément est déjà existant."}
        response = JsonResponse(data=data, status=status)
    return response


def delete_func(id, user, model):
    # Suppression d'un élément et formatage d'une réponse en Json --
    # Implementer pour sourceID, contextID, filterID, analyzerID, TokenizerID, SearchModelID

    if model is Source:
        obj = get_object_or_404(model, id=id)

        if obj.user == user:
            obj.delete()
            data = {}
            status = 204
        else:
            data = {"error": "Echec de la suppression: Vous n'etes pas l'usager de cet élément."}
            status = 403

    if model is Context:
        # l'user n'est accessible qu'au travers de la source de la resource du context :)
        context = Context.objects.filter(resource_id=id, resource__source__user=user)
        if len(context) == 1:
            context.delete()
            data = {}
            status = 204
        elif len(context) == 0 and Context.objects.filter(resource_id=id).count() > 0:
            data = {"error": "Echec de la suppression: Vous n'etes pas l'usager de cet élément."}
            status = 403

    if model in [Filter, Analyzer, Tokenizer, SearchModel]:

        obj = get_object_or_404(model, name=id)
        if obj.user == user:
            # On a besoin de verifier le champs reserved à False si existant dans le model.
            try:
                model._meta.get_field("reserved")
            except FieldDoesNotExist:
                obj.delete()
                status = 204
                data = {}
            else:
                if not obj.reserved:
                    obj.delete()
                    status = 204
                    data = {}
                else:
                    status = 405
                    data = {"error": "Suppression impossible: L'usage de cet élément est réservé."}
        else:
            status = 403
            data = {"error": "Suppression impossible: Vous n'etes pas l'usager de cet élément."}

    return JsonResponse(data, status=status)


def user_access(name, model, usr_req):
    # Check si user() == obj.user
    # Implementé pour filterID, analyserID, tokenizerID, SearchModelID

    try:
        obj = model.objects.get(name=name)
    except ObjectDoesNotExist:
        data = "Aucun objet %s ne correspond à la requête." % model
        status = 404
        raise JsonError(message=data, status=status)
    if model._meta.get_field("user"):
        try:
            obj = model.objects.get(name=name, user=usr_req)
        except ObjectDoesNotExist:
            data = "Vous n'etes pas l'usager de cet élément."
            status = 403
            raise JsonError(message=data, status=status)


def clean_my_obj(obj):
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(clean_my_obj(x) for x in obj if x is not None)
    elif isinstance(obj, dict):
        return type(obj)((clean_my_obj(k), clean_my_obj(v))
                         for k, v in obj.items() if k is not None and v is not None)
    else:
        return obj
