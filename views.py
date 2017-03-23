from ast import literal_eval
from re import search
from uuid import uuid4

from django.views.generic import View
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse, HttpResponse
from django.core.exceptions import ValidationError
from django.shortcuts import get_object_or_404
from .models import Source, Resource, Context, Filter, Analyzer, Tokenizer, SearchModel
from django.conf import settings
from django.db import transaction, IntegrityError


from onegeo_manager.source import PdfSource
from onegeo_manager.index import Index
from onegeo_manager.context import PdfContext

import json

from . import utils

from .elasticsearch_wrapper import elastic_conn

PDF_BASE_DIR = settings.PDF_DATA_BASE_DIR
MSG_406 = "Le format demandé n'est pas pris en charge."


@method_decorator(csrf_exempt, name="dispatch")
class SourceView(View):

    def get(self, request):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        return JsonResponse(utils.get_objects(user(), Source), safe=False)


    def post(self, request):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user

        if "application/json" not in request.content_type:
            data = {"error": MSG_406}
            return JsonResponse(data, status=406)
        data = request.body.decode('utf-8')
        body_data = json.loads(data)

        field_missing = False
        if "uri" not in body_data:
            data = {"error": "Echec de la création de la source. Le chemin d'accés à la source est manquant."}
            field_missing = True
        if "mode" not in body_data:
            data = {"error": "Echec de la création de la source. Le type de source est manquant (ex:mode=pdf)."}
            field_missing = True
        if "name" not in body_data:
            data = {"error": "Echec de la création de la source. Le nom de la source est manquant."}
            field_missing = True
        if field_missing is True:
            return JsonResponse(data, status=400)

        name = utils.read_name(body_data)
        if name is None:
            return JsonResponse({"error": "Echec de la création du contexte. Le nom du context est incorrect."},
                                status=400)
        uri = body_data["uri"]
        mode = body_data["mode"]

        np = utils.check_uri(uri)
        if np is None:
            data = {"error": "Echec de la création de la source. Le chemin d'accés à la source est incorrect."}
            return JsonResponse(data, status=400)


        sources, created = Source.objects.get_or_create(uri=np,defaults={'user': user(),
                                                                         'name':name,
                                                                         'mode':mode})

        status = created and 201 or 409
        return utils.format_json_get_create(request, created, status, sources.id)


@method_decorator(csrf_exempt, name="dispatch")
class SourceIDView(View):
    def get(self, request, id):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        src_id = literal_eval(id)
        return JsonResponse(utils.get_object_id(user(), src_id, Source), safe=False)

    def delete(self, request, id):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        id = literal_eval(id)

        return utils.delete_func(id, user(), Source)


@method_decorator(csrf_exempt, name="dispatch")
class ResourceView(View):
    def get(self, request, id):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user

        src_id = literal_eval(id)
        return JsonResponse(utils.get_objects(user(), Resource, src_id), safe=False)


@method_decorator(csrf_exempt, name="dispatch")
class ResourceIDView(View):
    def get(self, request, src_id, rsrc_id):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        return JsonResponse(utils.get_object_id(user(), rsrc_id, Resource, src_id), safe=False)


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
            return JsonResponse({"error": "Echec de la création du contexte. Le nom du contexte est manquant."}, status=400)
        if "resource" not in body_data:
            return JsonResponse({"error": "Echec de la création du contexte. Le chemin d'accés à la resource est manquant pour le context."}, status=400)

        name = utils.read_name(body_data)
        if name is None:
            return JsonResponse({"error": "Echec de la création du contexte. Le nom du context est incorrect."}, status=400)
        if Context.objects.filter(name=name).count() > 0:
            return JsonResponse({"error": "Echec de la création du contexte. Le nom d'un context doit etre unique"}, status=409)

        reindex_frequency = "monthly"
        if "reindex_frequency" in body_data:
            reindex_frequency = body_data['reindex_frequency']

        data = search('^\/sources\/(\d+)\/resources\/(\d+)$', body_data['resource'])
        if not data:
            return None
        src_id = data.group(1)
        rsrc_id = data.group(2)
        set_src = get_object_or_404(Source, id=src_id)

        set_rscr = get_object_or_404(Resource, source=set_src, id=rsrc_id)
        if Context.objects.filter(resource__id=rsrc_id).count() > 0:
            return JsonResponse({"error": "Echec de la création du contexte. Cette resource est déja liée à un context"}, status=409)

        pdf = PdfSource(set_src.uri, name, set_src.mode)
        type = None
        index = Index(set_rscr.name)
        for e in iter(pdf.get_types()):
            if e.name == set_rscr.name:
                type = e
        context = PdfContext(index, type)
        column_ppt = []
        for property in context.iter_properties():
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
        return JsonResponse(utils.get_object_id(user(), ctx_id, Context), safe=False)

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

        data = search('^\/sources\/(\d+)\/resources\/(\d+)$', body_data['resource'])
        if not data:
            return None
        src_id = data.group(1)
        rsrc_id = data.group(2)
        set_src = get_object_or_404(Source, id=src_id)
        set_rscr = get_object_or_404(Resource, source=set_src, id=rsrc_id)

        ctx_id = literal_eval(id)
        context = get_object_or_404(Context, resource_id=ctx_id)

        list_ppt = context.clmn_properties
        ppt_update = utils.check_columns(list_ppt, list_ppt_clt)

        if reindex_frequency:
            context.resource = set_rscr
            context.name = name
            context.clmn_properties = ppt_update
            context.reindex_frequency = reindex_frequency
            context.save()
        else:
            context.resource = set_rscr
            context.name = name
            context.clmn_properties = ppt_update
            context.save()

        return JsonResponse(data={}, status=200)

    def delete(self, request, id):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user

        id = literal_eval(id)

        return utils.delete_func(id, user(), Context)


@method_decorator(csrf_exempt, name="dispatch")
class FilterView(View):
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
            return JsonResponse({"error": "Echec de la création du filtre: Le nom du filtre est incorrect."}, status=400)
        if Filter.objects.filter(name=name).count() > 0:
            return JsonResponse({"error": "Echec de la création du filtre: Le nom du filtre doit etre unique."}, status=409)

        cfg = "config" in body_data and body_data["config"] or {}

        filter, created = Filter.objects.get_or_create(name=name, defaults={"config":cfg,
                                                                           "user":user()})
        status = created and 201 or 409
        return utils.format_json_get_create(request, created, status, filter.name)


@method_decorator(csrf_exempt, name="dispatch")
class FilterIDView(View):
    def get(self, request, name):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        name = (name.endswith('/') and name[:-1] or name)

        try:
            utils.user_access(name, Filter, user())
        except utils.JsonError as e:
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
            status = 200
            data = {}
        elif len(filter) == 0:
            flt = Filter.objects.filter(name=flt_name)
            if len(flt) == 1:
                status = 403
                data = {"error": "Modification impossible: Vous n'etes pas l'usager de ce filtre"}
            elif len(flt) == 0:  #Cas exclu car
                status = 204
                data = {"message": "Modification impossible: Aucun filtre ne correspond à votre requête."}

        return JsonResponse(data, status=status)

    def delete(self, request, name):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        name = (name.endswith('/') and name[:-1] or name)
        return utils.delete_func(name, user(), Filter)


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
            return JsonResponse({"error": "Echec de la création: Le nom de l'analyseur est manquant."}, status=400)
        if Analyzer.objects.filter(name=name).count() > 0:
            return JsonResponse({"error": "Echec de la création: Le nom de l'analyseur doit etre unique."}, status=409)

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
                    return JsonResponse({"error": "Echec de la création de l'analyseur: La liste des filtres est erronée."}, status=400)

        if created and tokenizer is not None:
            try:
                tkn_chk = Tokenizer.objects.get(name=tokenizer)
                analyzer.tokenizer = tkn_chk
                analyzer.save()
            except Tokenizer.DoesNotExist:
                return JsonResponse({"error": "Echec de la création de l'analyseur: Le token est erronée"}, status=400)
        status = created and 201 or 409
        return utils.format_json_get_create(request, created, status, analyzer.name)


@method_decorator(csrf_exempt, name="dispatch")
class AnalyzerIDView(View):
    def get(self, request, name):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        name = (name.endswith('/') and name[:-1] or name)

        try:
            utils.user_access(name, Analyzer, user())
        except utils.JsonError as e:
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
                return JsonResponse({"error": "Echec de la modification de l'analyseur: Le token est erronée"}, status=400)

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
                    return JsonResponse({"error": "Echec de la modification de l'analyseur: La liste des filtres est erronée."},
                                        status=400)
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
            return JsonResponse({"error": "Echec de la création du token: Le nom du token est manquant."}, status=400)

        cfg = "config" in body_data and body_data["config"] or {}

        token, created = Tokenizer.objects.get_or_create(name=name, defaults={"config":cfg,
                                                                              "user":user()})
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
        except utils.JsonError as e:
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
        except utils.JsonError as e:
            return JsonResponse(data={"error": e.message}, status=e.status)

        Tokenizer.objects.filter(name=name).update(config=cfg)
        return JsonResponse(data={}, status=204)

    def delete(self, request, name):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        name = (name.endswith('/') and name[:-1] or name)

        return utils.delete_func(name, user(), Tokenizer)


@method_decorator(csrf_exempt, name="dispatch")
class Directories(View):
    def get(self, request):
        user = utils.UserAuthenticate(request)
        if user() is None:
            response = HttpResponse()
            response.status_code = 401
            response['WWW-Authenticate'] = 'Basic realm="%s"' % "Basic Auth Protected"
            return response

        subdir = utils.uri_shortcut(PDF_BASE_DIR)

        return JsonResponse(subdir, safe=False)


@method_decorator(csrf_exempt, name="dispatch")
class ActionView(View):

    def post(self, request):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user

        data = json.loads(request.body.decode("utf-8"))

        try:
            ctx = Context.objects.get(name=data["index"])
        except Context.DoesNotExist:
            return JsonResponse({"error": "Le context est erroné"}, status=404)

        if elastic_conn.is_a_task_running():
            data = {"error": "Accés verouillé: une autre tâche est en cours d'exécution"}
            return JsonResponse(data, status=423)

        action = data["type"]

        rscr = ctx.resource
        src = rscr.source

        pdf = PdfSource(src.uri, src.name, src.mode)

        doc_type = None
        index = Index(rscr.name)
        for e in iter(pdf.get_types()):
            if e.name == rscr.name:
                doc_type = e

        context = PdfContext(index, doc_type)

        for col_property in iter(ctx.clmn_properties):
            context_name = col_property.pop('name')
            context.update_property(context_name, **col_property)

        opts = {}

        if src.mode == "pdf":
            pipeline = "attachment"
            elastic_conn.create_pipeline_if_not_exists(pipeline)
            opts.update({"pipeline": pipeline})

        if action == "rebuild":
            opts.update({"collections": context.get_collection()})

        if action == "reindex":
            pass  # Action par défaut

        body = {'mappings': context.generate_elastic_mapping(),
                'settings': {
                    'analysis': self.retreive_analysis(
                        self.retreive_analyzers(context))}}

        elastic_conn.create_or_replace_index(str(uuid4())[0:7],  # Un UUID comme nom d'index
                                             ctx.name,  # Alias de l'index
                                             doc_type.name,  # Nom du type
                                             body,  # Settings & Mapping
                                             **opts)

        status = 202
        data = {"message": "Requete acceptée mais sans garantie de traitement"}
        return JsonResponse(data, status=status)

    def retreive_analyzers(self, context):

        analyzers = []
        for prop in context.iter_properties():
            if prop.analyzer not in analyzers:
                analyzers.append(prop.analyzer)
            if prop.search_analyzer not in analyzers:
                analyzers.append(prop.search_analyzer)
        return [analyzer for analyzer in analyzers if analyzer not in (None, '')]

    def retreive_analysis(self, analyzers):

        analysis = {'analyzer': {}, 'filter': {}, 'tokenizer': {}}

        for analyzer_name in analyzers:
            analyzer = Analyzer.objects.get(name=analyzer_name)

            plug_anal = analyzer.filter.through
            if analyzer.reserved:
                if plug_anal.objects.filter(analyzer__name=analyzer_name) and analyzer.tokenizer:
                    pass
                else:
                    continue

            analysis['analyzer'][analyzer.name] = {'type': 'custom'}

            tokenizer = analyzer.tokenizer

            if tokenizer:
                analysis['analyzer'][analyzer.name]['tokenizer'] = tokenizer.name
                if tokenizer.config:
                    analysis['tokenizer'][tokenizer.name] = tokenizer.config

            filters_name = utils.iter_flt_from_anl(analyzer.name)

            for filter_name in iter(filters_name):
                filter = Filter.objects.get(name=filter_name)
                if filter.config:
                    analysis['filter'][filter.name] = filter.config

            analysis['analyzer'][analyzer.name]['filter'] = filters_name

        return analysis


@method_decorator(csrf_exempt, name="dispatch")
class SearchModelView(View):
    def get(self, request):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        return JsonResponse(utils.get_objects(user(), SearchModel), safe=False)

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
            return JsonResponse({"error": "Echec de la création du model de recherche: Le nom du model de recherche est incorrect."}, status=400)
        if SearchModel.objects.filter(name=name).count() > 0:
            return JsonResponse({"error": "Echec de la création du model de recherche: Le nom du model de recherche doit etre unique."}, status=409)

        cfg = "config" in body_data and body_data["config"] or {}
        ctx = "contexts" in body_data and body_data["contexts"] or []

        try:
            search_model, created = SearchModel.objects.get_or_create(config=cfg, user=user(), name=name)
        except ValidationError as e:
            return JsonResponse({"error": e.message}, status=409)

        status = created and 201 or 409
        if created:
            search_model.context.clear()
            ctx_clt = []
            if len(ctx) > 0:
                ctx_l = []
                for c in ctx_clt:
                    try:
                        ctx = Context.objects.get(name=c)
                    except Context.DoesNotExist:
                        return JsonResponse({"error": "Echec de la création du model de recherche: La liste de contexte est erronée"},
                                            status=400)
                    else:
                        ctx_l.append(ctx)
                search_model.context.set(ctx_l)

            response = JsonResponse(data={}, status=status)
            response['Location'] = '{}{}'.format(request.build_absolute_uri(), search_model.name)
        if created is False:
            data = {"error": "Conflict"}
            response = JsonResponse(data=data, status=status)
        return response


@method_decorator(csrf_exempt, name="dispatch")
class SearchModelIDView(View):
    def get(self, request, name):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user
        name = (name.endswith('/') and name[:-1] or name)

        try:
            utils.user_access(name, SearchModel, user())
        except utils.JsonError as e:
            return JsonResponse(data={"error": e.message}, status=e.status)

        return JsonResponse(utils.get_object_id(user(), name, SearchModel), status=200)

    def put(self, request, name):

        # Check user
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user

        # Check content-type
        if "application/json" not in request.content_type:
            return JsonResponse({"Error": MSG_406}, status=406)
        data = request.body.decode('utf-8')
        body_data = json.loads(data)

        name = (name.endswith('/') and name[:-1] or name)

        ctx_clt = "contexts" in body_data and body_data["contexts"] or []
        config = "config" in body_data and body_data["config"] or {}

        if elastic_conn.is_a_task_running():
            status = 423
            data = {"error": "Accés verouillé: une autre tâche est en cours d'exécution"}
            return JsonResponse(data, status=status)

        try:
            utils.refresh_search_model(name, ctx_clt)
        except RuntimeError:
            status = 423
            data = {"error": "Accés verouillé: une autre tâche est en cours d'exécution"}
            return JsonResponse(data, status=status)


        search_model = SearchModel.objects.filter(name=name, user=user())
        if len(search_model) == 1:

            sm = get_object_or_404(SearchModel, name=name) #get object pour sm.context.set

            sm.context.clear()
            ctx_l = []
            for c in ctx_clt:
                try:
                    ctx = Context.objects.get(name=c)
                except Context.DoesNotExist:
                    return JsonResponse(
                        {"error": "Echec de la modification du model de recherche: La liste de contexte est erronée"},
                        status=400)
                else:
                    ctx_l.append(ctx)
            sm.context.set(ctx_l)

            search_model.update(config=config)
            status = 200
            data = {}

        elif len(search_model) == 0:
            mdl = SearchModel.objects.filter(name=name)

            if len(mdl) == 1:
                status = 403
                data = {"error": "Modification du model de recherche impossible: Son usage est reservé."}

            elif len(mdl) == 0:
                status = 204 # Code erreur 404 pour une resource inexistante mais cas impossible cf sm = get_object_or_404(SearchModel, name=name)
                data = {"message": "Modification du model de recherche impossible: Aucun model de recherche ne correspond."}

        return JsonResponse(data, status=status)

    def delete(self, request, name):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user

        name = (name.endswith('/') and name[:-1] or name)
        return utils.delete_func(name, user(), SearchModel)


@method_decorator(csrf_exempt, name="dispatch")
class SearchView(View):
    def post(self, request, name):
        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user

        model = SearchModel.objects.filter(name=name)

        data = request.body.decode('utf-8')

        mode = utils.get_param(request, 'mode')
        if mode == 'throw':
            data = elastic_conn.search(index=name, body=data)
            if data:
                return JsonResponse(data=data, safe=False, status=200)
        else:
            return JsonResponse(data={'message': 'todo'}, safe=False, status=501)

