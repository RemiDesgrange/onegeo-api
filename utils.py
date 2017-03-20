import functools
from base64 import b64decode
from pathlib import Path
from re import search

from .models import Source, Resource, Context, Filter, Analyzer, Tokenizer, SearchModel
from django.contrib.auth import authenticate
from django.contrib.auth.models import User
from django.conf import settings
from django.shortcuts import get_object_or_404
from django.http import HttpResponse, JsonResponse
from django.db import IntegrityError
from django.db.models import Q
from django.core.exceptions import FieldDoesNotExist, ObjectDoesNotExist


from .elasticsearch_wrapper import elastic_conn


PDF_BASE_DIR = settings.PDF_DATA_BASE_DIR


def format_resource(s, r):
    try:
        ctx = Context.objects.get(resource_id=r.id)
        d = {'id': r.id,
             'location': '/sources/{}/resources/{}'.format(s.id, r.id),
             'name': r.name,
             'columns': r.columns,
             'context': format_context(s, r, ctx)["location"]}
    except Context.DoesNotExist:
        d = {'id': r.id,
             'location': '/sources/{}/resources/{}'.format(s.id, r.id),
             'name': r.name,
             'columns': r.columns
             }
    return d


def format_source(s):
    d = {'id': s.id,
         'uri': s.s_uri,
         'mode': s.mode,
         'name': s.name,
         'location': '/sources/{}'.format(s.id),
         'resources': [format_resource(s, r) for r in list(Resource.objects.filter(source=s).order_by('name'))]}
    return d


def format_context(s, r, c):
    d = {"location": "/contexts/{}".format(c.resource_id),
         "resource": "/sources/{}/resources/{}".format(s.id, r.id),
         "columns": c.clmn_properties,
         "name": c.name,
         "reindex_frequency": c.reindex_frequency
    }
    return d


def format_filter(obj):
    return clean_my_obj({
        "location": "filters/{}".format(obj.name),
        "name": obj.name,
        "config": obj.config or None,
        "reserved": obj.reserved})


def iter_flt_from_anl(anl_name):
    AnalyserFilters = Analyzer.filter.through
    set = AnalyserFilters.objects.filter(analyzer__name=anl_name).order_by('id')
    return [s.filter.name for s in set if s.filter.name is not None]


def format_analyzer(obj):
    return clean_my_obj({
        "location": "analyzers/{}".format(obj.name),
        "name": obj.name,
        "filters": iter_flt_from_anl(obj.name) or None,
        "reserved": obj.reserved,
        "tokenizer": obj.tokenizer and obj.tokenizer.name or None})


def format_tokenizer(obj):
    return clean_my_obj({
        "location": "tokenizers/{}".format(obj.name),
        "name": obj.name,
        "config": obj.config or None,
        "reserved": obj.reserved})


def iter_ctx_from_search_model(mdl_name):
    SMC = SearchModel.context.through
    set = SMC.objects.filter(searchmodel__name=mdl_name)
    return [s.context.name for s in set if s.context.name is not None]

def iter_mdl_from_ctx_name(ctx_name):
    SMC = SearchModel.context.through
    set = SMC.objects.filter(context__name=ctx_name)
    return [s.searchmodel.name for s in set if s.searchmodel.name is not None]

def format_search_model(obj):
    l = iter_ctx_from_search_model(obj.name)
    return {
        "location": "models/{}".format(obj.name),
        "name": obj.name,
        "config": obj.config,
        "contexts": l
    }


# Formate la réponse Json selon le type de model pour un ensemble d'objets
def get_objects(user, mdl, src_id=None):
    l = []
    d = {Tokenizer: format_tokenizer,
         Analyzer: format_analyzer,
         Filter: format_filter}

    if mdl in d:
        obj = mdl.objects.filter(Q(user=user) | Q(user=None)).order_by('reserved', 'name')
        for o in obj:
            l.append(d[mdl](o))

    if mdl is SearchModel:
        search_model = SearchModel.objects.filter(Q(user=user) | Q(user=None)).order_by('name')
        for sm in search_model:
            l.append(format_search_model(sm))

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
        rsrc = Resource.objects.filter(source=source, source__user=user).order_by('name')
        for r in rsrc:
            l.append(format_resource(source, r))

    if mdl is Source:
        src = Source.objects.filter(user=user).order_by('name')
        for s in src:
            l.append(format_source(s))

    return l


# Formate la réponse Json selon le type de model pour un objet identifié
def get_object_id(user, id, mdl, src_id=None):
    l = {}
    d = {SearchModel : format_search_model,
         Tokenizer: format_tokenizer,
         Analyzer : format_analyzer,
         Filter : format_filter}

    if mdl in d:
        obj = get_object_or_404(mdl, name=id)
        if obj.user == user or obj.user is None:
            l = d[mdl](obj)

    if mdl is Context:
        src = Source.objects.filter(user=user)
        for s in src:
            rsrc = Resource.objects.filter(source=s, source__user=user)
            for r in rsrc:
                if r.id == id:
                    ctx = Context.objects.get(resource_id=r.id, resource__source__user=user)
                    l = format_context(s, r, ctx)

    if mdl is Resource and src_id is not None:
        source = get_object_or_404(Source, id=src_id, user=user)
        rsrc = get_object_or_404(Resource, id=id, source=source, source__user=user)
        l = format_resource(source, rsrc)

    if mdl is Source:
        source = get_object_or_404(Source, id=id, user=user)
        l = format_source(source)

    return l


def read_name(body_data):
    if "name" not in body_data or body_data["name"] == "":
        return None
    try:
        name = search("^[a-z0-9_]{2,30}$", body_data["name"])
        name = name.group(0)
    except AttributeError:
        return None
    return name


def uri_shortcut(b):
    """
    Retourne une liste d'uri sous la forme de "file:///dossier",
    afin de caché le chemin absolue des sources
    """
    p = Path(b.startswith("file://") and b[7:] or b)
    if not p.exists():
        raise ConnectionError('Given path does not exist.')
    l = []
    for x in p.iterdir():
        if x.is_dir():
            l.append('file:///{}'.format(x.name))
    return l


def check_uri(b):
    """
    Verifie si l'uri en param d'entrée recu sous la forme de "file:///dossier"
    Correspond a un des dossiers enfant du dossier parent PDF_BASE_DIR
    Retourne l'uri complete si correspondance, None sinon.
    NB: l'uri complete sera verifier avant tout action save() sur modele Source()
    """
    p = Path(PDF_BASE_DIR)
    if not p.exists():
        raise ConnectionError('Given path does not exist.')
    for x in p.iterdir():
        if x.is_dir() and x.name == b[8:]:
            return x.as_uri()
    return None


class HttpResponseUnauthorized(HttpResponse):
    """
        Surcharge HttpResponse pour retourner des erreur 401
    """
    status_code = 401


def basic_authenticate(fct):
    """USAGE views.py/functions:
        @utils.basic_authenticate
    """
    @functools.wraps(fct)  # Permet de transmettre les attributs
    def wrapper_fct(*args, **kwargs):
        if 'HTTP_AUTHORIZATION' in args[0].request.META:
            auth = args[0].request.META['HTTP_AUTHORIZATION'].split()
            if len(auth) == 2:
                if auth[0].lower() == "basic":
                    uname, passwd = b64decode(auth[1]).decode("utf-8").split(':')
                    user = authenticate(username=uname, password=passwd)
                    if user:
                        return fct(*args, **kwargs)
                    else:
                        return HttpResponseUnauthorized()

        response = HttpResponse()
        response.status_code = 401
        response['WWW-Authenticate'] = 'Basic realm="%s"' % "Basic Auth Protected"
        return response

    return wrapper_fct


# Authentification clé api + droits liés
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

    def __init__(self, request):
        self.request = request

    def __enter__(self):
        if 'HTTP_AUTHORIZATION' in self.request.META:
            auth = self.request.META['HTTP_AUTHORIZATION'].split()
            if len(auth) == 2:
                if auth[0].lower() == "basic":
                    uname, passwd = b64decode(auth[1]).decode("utf-8").split(':')
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
        response['WWW-Authenticate'] = 'Basic realm="%s"' % "Basic Auth Protected"
        return response
    return user

def check_columns(list_ppt, list_ppt_clt):
    for ppt in list_ppt:
        for ppt_clt in list_ppt_clt:
            if ppt['name'] == ppt_clt['name']:
                ppt.update(ppt_clt)
    return list_ppt

def get_param(request, param):
    """
        Retourne la valeur d'une clé param presente dans une requete GET ou POST
    """
    if request.method == 'GET':
        if param in request.GET:
            return request.GET[param]
    elif request.method == 'POST':
        try:
            param_read = request.POST.get(param, request.GET.get(param))
        except KeyError as e:
            return None
        return param_read


# Format response Json apres les get_or_create()
def format_json_get_create(request, created, status, obj_id):
    if created:
        response = JsonResponse(data={}, status=status)
        response['Location'] = '{}{}'.format(request.build_absolute_uri(), obj_id)
    if created is False:
        data = {"error": "Echec de la création: L'élément est déjà existant."}
        response = JsonResponse(data=data, status=status)
    return response


# Suppression d'un élément et formatage d'une réponse en Json --
# Implementer pour sourceID, contextID, filterID, analyzerID, TokenizerID, SearchModelID
def delete_func(id, user, model):
    # CF: https: // www.w3.org / Protocols / rfc2616 / rfc2616 - sec9.html

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



class JsonError(Exception):
    def __init__(self, message, status):
        super().__init__(message, status)
        self.message = message
        self.status = status

# Check si user() == obj.user -- Implementé pour filterID, analyserID, tokenizerID, SearchModelID
def user_access(name, model, usr_req):

    try:
        obj = model.objects.get(name=name)
    except ObjectDoesNotExist:
        data = {'error': 'Aucun objet %s ne correspond à la requête.' % model}
        status = 404
        raise JsonError(data, status=status)
    if model._meta.get_field("user"):
        try:
            obj = model.objects.get(name=name, user=usr_req)
        except ObjectDoesNotExist:
            data = {"error": "Vous n'etes pas l'usager de cet élément."}
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


class MultiTaskError(Exception):
    def __init__(self, message):
        super().__init__(message)


def refresh_search_model(mdl_name, ctx_name_l):
    """
    Mise à jour des index d'un modele de recherche et des anciens dictionnaires index/name_model.

    """
    body = {'actions': []}


    for index in elastic_conn.get_indices_by_alias(name=mdl_name):
        body['actions'].append({'remove': {'index': index, 'alias': mdl_name}})

    for context in iter(ctx_name_l):
        for index in elastic_conn.get_indices_by_alias(name=context):
            body['actions'].append({'add': {'index': index, 'alias': mdl_name}})

    if not elastic_conn.is_a_task_running():
        elastic_conn.update_aliases(body)
    else:
        raise RuntimeError

