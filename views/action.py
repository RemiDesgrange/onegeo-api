import json
from django.conf import settings
from django.http import JsonResponse, HttpResponse
from django.utils import timezone
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.views.generic import View
from onegeo_manager.context import Context as OnegeoContext
from onegeo_manager.index import Index as OnegeoIndex
from onegeo_manager.resource import Resource as OnegeoResource
from onegeo_manager.source import Source as OnegeoSource
from uuid import uuid4

from .. import utils
from ..elasticsearch_wrapper import elastic_conn
from ..models import Context, Filter, Analyzer, Task


__all__ = ["ActionView"]


PDF_BASE_DIR = settings.PDF_DATA_BASE_DIR


def iter_flt_from_anl(anl_name):
    AnalyserFilters = Analyzer.filter.through
    set = AnalyserFilters.objects.filter(analyzer__name=anl_name).order_by("id")
    return [s.filter.name for s in set if s.filter.name is not None]


@method_decorator(csrf_exempt, name="dispatch")
class ActionView(View):

    def _retreive_analysis(self, analyzers):

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

            filters_name = iter_flt_from_anl(analyzer.name)

            for filter_name in iter(filters_name):
                filter = Filter.objects.get(name=filter_name)
                if filter.config:
                    analysis['filter'][filter.name] = filter.config

            analysis['analyzer'][analyzer.name]['filter'] = filters_name

        return analysis

    def _retreive_analyzers(self, context):

        analyzers = []
        for prop in context.iter_properties():
            if prop.analyzer not in analyzers:
                analyzers.append(prop.analyzer)
            if prop.search_analyzer not in analyzers:
                analyzers.append(prop.search_analyzer)
        return [analyzer for analyzer in analyzers if analyzer not in (None, '')]

    def post(self, request):

        user = utils.get_user_or_401(request)
        if isinstance(user, HttpResponse):
            return user

        data = json.loads(request.body.decode("utf-8"))

        try:
            ctx = Context.objects.get(name=data["index"])
        except Context.DoesNotExist:
            return JsonResponse({"error": "Le contexte d'indexation n'existe pas. "}, status=404)

        filters = {"model_type": "context", "model_type_id": ctx.pk, "user": user()}
        last = Task.objects.filter(**filters).order_by("start_date").last()
        if last and last.success is None:
            data = {"error": "Une autre tâche est en cours d'exécution. "
                             "Veuillez réessayer plus tard. "}
            return JsonResponse(data, status=423)

        action = data["type"]

        rscr = ctx.resource
        src = rscr.source

        onegeo_source = OnegeoSource(src.uri, src.name, src.mode)
        onegeo_resource = OnegeoResource(onegeo_source, rscr.name)
        for column in iter(rscr.columns):
            if onegeo_resource.is_existing_column(column["name"]):
                continue
            onegeo_resource.add_column(column["name"], column_type=column["type"],
                                       occurs=tuple(column["occurs"]), count=column["count"])

        onegeo_index = OnegeoIndex(rscr.name)
        onegeo_context = OnegeoContext(ctx.name, onegeo_index, onegeo_resource)

        for col_property in iter(ctx.clmn_properties):
            context_name = col_property.pop('name')
            onegeo_context.update_property(context_name, **col_property)

        opts = {}

        if src.mode == "pdf":
            pipeline = "attachment"
            elastic_conn.create_pipeline_if_not_exists(pipeline)
            opts.update({"pipeline": pipeline})

        if action == "rebuild":
            opts.update({"collections": onegeo_context.get_collection()})

        if action == "reindex":
            pass  # Action par défaut

        body = {'mappings': onegeo_context.generate_elastic_mapping(),
                'settings': {
                    'analysis': self._retreive_analysis(
                        self._retreive_analyzers(onegeo_context))}}

        index_uuid = str(uuid4())[0:7]

        description = "Les données sont en cours d'indexation (id de l'index: '{0}'). ".format(
            index_uuid)
        tsk = Task.objects.create(model_type="context", description=description,
                                  user=user(), model_type_id=ctx.pk)

        def on_index_error(desc):
            pass

        def on_index_success(desc):
            tsk.success = True
            tsk.stop_date = timezone.now()
            tsk.description = desc
            tsk.save()

        def on_index_failure(desc):
            tsk.success = False
            tsk.stop_date = timezone.now()
            tsk.description = desc
            tsk.save()

        opts.update({"error": on_index_error,
                     "failed": on_index_failure,
                     "succeed": on_index_success})

        elastic_conn.create_or_replace_index(
                    index_uuid, ctx.name, ctx.name, body, **opts)

        status = 202
        data = {"message": tsk.description}

        return JsonResponse(data, status=status)
