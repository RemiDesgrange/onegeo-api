import json

from django.test import TestCase, RequestFactory, Client
from .models import Source, Resource, Context, Filter, Analyzer
from django.contrib.auth.models import User, AnonymousUser
from .views import (SourceView, SourceIDView, ResourceView,
                    ResourceIDView, Directories, ContextView,
                    ContextIDView, FilterView, FilterIDView,
                    AnalyzerView, AnalyzerIDView, TokenizerView,
                    TokenizerIDView, ActionView, SearchModelView,
                    SearchModelIDView, SearchView)

from .utils import *

class SimpleTest(TestCase):


    def setUp(self):
        self.factory = RequestFactory()

        self.user = User.objects.create_user(
            username='user1', email='user_test@testing.com', password='passpass')

        # creer user2 et objets liés pour tester accés
        self.user2 = User.objects.create_user(
            username='user2', email='user_test@testing.com', password='passpass')

        full_uri = check_uri('file:///RAAD')
        self.name_source_legit = "raad0"
        Source.objects.create(uri=full_uri, user=self.user, name=self.name_source_legit)

        self.name_filter_legit = "namefilter"
        Filter.objects.create(name=self.name_filter_legit, user=self.user)
        self.filter = Filter.objects.get(name=self.name_filter_legit)

        self.name_filter_legit2 = "namefilter2"
        Filter.objects.create(name=self.name_filter_legit2, user=self.user)
        self.filter2 = Filter.objects.get(name=self.name_filter_legit2)


        self.name_analyzer_legit = "nameanalyzer"
        Analyzer.objects.create(name=self.name_analyzer_legit, user=self.user)
        self.analyzer = Analyzer.objects.get(name=self.name_analyzer_legit)

        Analyzer.objects.create(name="nameanalyzerdelete", user=self.user)


        self.name_token_legit = "nametoken"
        Tokenizer.objects.create(name=self.name_token_legit, user=self.user)
        self.token = Tokenizer.objects.get(name=self.name_token_legit)

        Tokenizer.objects.create(name="nametokendelete", user=self.user)

        Tokenizer.objects.create(name="nametoken_usr2", user=self.user2)


    #########################
    #        SOURCE         #
    #########################
    def test_source_get__with_anonymous_user(self):
        rf = self.factory
        request = rf.get('/api/sources/')
        request.user = AnonymousUser()
        response = SourceView.as_view()(request)
        self.assertEqual(response.status_code, 401)


    def test_source_get(self):
        rf = self.factory
        request = rf.get('/api/sources/')

        # l'authetification passe par une classe qui vérifie le contenu de META[]
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M=' #base64(user1:passpass)

        response = SourceView.as_view()(request)
        self.assertEqual(response.status_code, 200)


    def test_source_post_create(self):

        # Create source
        json_str = json.dumps({"uri": "file:///LYVIA",
                                 "mode": "pdf",
                                 "name": "lyvia0"
                               })
        rf = self.factory
        request = rf.post('/api/sources/',
                                    data=json_str,
                                    content_type="application/json")
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='

        response = SourceView.as_view()(request)
        self.assertEqual(response.status_code, 201)

        src = Source.objects.filter(user=self.user, name="lyvia0")
        self.assertEqual(src.count(), 1)

        src_sum = Source.objects.filter(user=self.user)
        self.assertEqual(src_sum.count(), 2)

    def test_source_id_get(self):

        id = Source.objects.get(user=self.user, name=self.name_source_legit).id

        rf = self.factory
        request2 = rf.get('/api/sources/{}'.format(id))
        # l'authetification passe par une classe qui vérifie le contenu de META[]
        request2.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='  # base64(user1:passpass)

        response2 = SourceView.as_view()(request2)
        # print(response2.content)
        self.assertEqual(response2.status_code, 200)


    def test_source_delete(self):
        rf = self.factory

        # Create source
        json_str = json.dumps({"uri": "file:///LYVIA",
                                 "mode": "pdf",
                                 "name": "lyvia1"
                               })
        request = rf.post('/api/sources/',
                                    data=json_str,
                                    content_type="application/json")
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='

        response = SourceView.as_view()(request)
        self.assertEqual(response.status_code, 201)

        src = Source.objects.get(user=self.user, name="lyvia1")

        rf2 = self.factory
        request2 = rf2.delete('/api/sources/{}'.format(src.pk))
        request2.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='

        response2 = SourceIDView.as_view()(request2, id=str(src.pk))
        self.assertEqual(response2.status_code, 204)


    #########################
    #        CONTEXTE       #
    #########################
    def test_contexts_post(self):
        # Utilisation d'une source dont la creation est garentie et de sa resource liée
        # Les tests étant réaliser sans garenti d'ordre, on s'assure d'avoir un couple source/resource disponible

        src = Source.objects.get(user=self.user, name=self.name_source_legit)
        rsrc = Resource.objects.filter(source=src)

        ctx_post_data = {"name": "ctxnewname",
                             "resource": "/sources/{}/resources/{}".format(src.id, rsrc[0].id),
                             "reindex_frequency": "daily"
                             }
        json_str = json.dumps(ctx_post_data)
        rf = self.factory
        request = rf.post('/api/contexts/',
                                    data=json_str,
                                    content_type="application/json")
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='

        response = ContextView.as_view()(request)
        self.assertEqual(response.status_code, 201)

        ctx = Context.objects.filter(name="ctxnewname")
        self.assertEqual(ctx.count(), 1)


    def test_context_id_get(self):
        src = Source.objects.get(user=self.user, name=self.name_source_legit)
        rsrc = Resource.objects.filter(source=src)
        print([r.name for r in rsrc])
        Context.objects.create(resource=rsrc[0], name="ctx_name", clmn_properties={"ppt1":"val1", "ppt2":"val2"})

        rf = self.factory
        request = rf.get('/api/contexts/{}'.format(rsrc[0].id))
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='

        response = ContextIDView.as_view()(request, id=str(rsrc[0].id))

        self.assertEqual(response.status_code, 200)

    def test_context_id_delete(self):
        rf = self.factory
        src = Source.objects.get(user=self.user, name=self.name_source_legit)
        rsrc = Resource.objects.filter(source=src)
        Context.objects.create(resource=rsrc[0], name="ctx_name2", clmn_properties={"ppt1": "val1", "ppt2": "val2"})

        request = rf.delete('/api/contexts/{}'.format(rsrc[0].id))
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='

        response = ContextIDView.as_view()(request, id=(str(rsrc[0].id)))
        self.assertEqual(response.status_code, 204)

    def test_context_id_delete_conflict_user(self):
        rf = self.factory
        user_alt = User.objects.create_user(
            username='user_alt', email='user_test@testing.com', password='passpass')

        full_uri = check_uri('file:///LYVIA')
        Source.objects.create(uri=full_uri, user=user_alt, name="lyvia2")
        src_alt = Source.objects.get(name="lyvia2")
        rsrc = Resource.objects.filter(source=src_alt)
        Context.objects.create(resource=rsrc[0], name="ctx_name3", clmn_properties={"ppt1": "val1", "ppt2": "val2"})

        request = rf.delete('/api/contexts/{}'.format(rsrc[0].id))
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='

        response = ContextIDView.as_view()(request, id=(str(rsrc[0].id)))
        self.assertEqual(response.status_code, 403)


    #########################
    #        FILTER         #
    #########################
    def test_filter_get_authent(self):

        rf1 = self.factory
        request1 = rf1.get('/api/filters')
        request1.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='  # base64(user1:passpass)
        response1 = FilterView.as_view()(request1)
        self.assertEqual(response1.status_code, 200)

    def test_filter_get_anonyme(self):

        rf2 = self.factory
        request1 = rf2.get('/api/filters')
        request1.META['HTTP_AUTHORIZATION'] = ''
        response1 = FilterView.as_view()(request1)
        self.assertEqual(response1.status_code, 401)



    def test_filter_post(self):
        rf = self.factory

        data = {"config": {"conf1": "val1"},
                "name": "namefilter3"}

        request = rf.post('/api/filters/',
                            data=json.dumps(data),
                            content_type="application/json")
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='

        response = FilterView.as_view()(request)
        self.assertEqual(response.status_code, 201)

    def test_filter_post(self):
        rf = self.factory

        data = {"config": {"conf1": "val1"},
                "name": "namefilter_anonyme"}

        request = rf.post('/api/filters/',
                            data=json.dumps(data),
                            content_type="application/json")
        request.META['HTTP_AUTHORIZATION'] = ''

        response = FilterView.as_view()(request)
        self.assertEqual(response.status_code, 401)


    def test_filter_post_bad_params(self):

        rf = self.factory
        data = {"config":{"conf1":"val1"},
                "name":"namefilter"}
        request = rf.post('/api/filters/',
                                    data=json.dumps(data),
                                    content_type="application/json")
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='
        response = FilterView.as_view()(request)
        self.assertEqual(response.status_code, 409)

        data2 = {"config": {"conf1": "val1"},
                "name": "N4M3- Bad"}
        request2 = rf.post('/api/filters/',
                          data=json.dumps(data2),
                          content_type="application/json")
        request2.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='
        response2 = FilterView.as_view()(request2)
        self.assertEqual(response2.status_code, 400)


    def test_filter_id_get(self):

        rf = self.factory
        name = "namefilter"
        request = rf.get('/api/filters/{}'.format(name))
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='

        response = FilterIDView.as_view()(request, name=str(name))

        self.assertEqual(response.status_code, 200)

    def test_filter_id_get_error_name(self):
        rf = self.factory
        name = "filter_unknown"
        request = rf.get('/api/filters/{}'.format(name))
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='

        response = FilterIDView.as_view()(request, name=str(name))

        self.assertEqual(response.status_code, 404)



    def test_filter_id_get_anonyme(self):
        rf = self.factory
        request = rf.get('/api/filters/{}'.format(self.filter.name))
        request.META['HTTP_AUTHORIZATION'] = ''
        response = FilterIDView.as_view()(request, name=str(self.filter.name))
        self.assertEqual(response.status_code, 401)



    #########################
    #       ANALYSEUR       #
    #########################
    def test_analyzer_get(self):
        rf = self.factory
        request = rf.get('/api/analyzers')
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='  # base64(user1:passpass)
        response = AnalyzerView.as_view()(request)
        self.assertEqual(response.status_code, 200)


    def test_analyzer_get_anonymous(self):
        rf = self.factory
        request = rf.get('/api/analyzers')
        request.META['HTTP_AUTHORIZATION'] = ''
        response = AnalyzerView.as_view()(request)
        self.assertEqual(response.status_code, 401)


    def test_analyzer_post(self):
        rf = self.factory

        # Utilisation du token_legit
        data = {"name":"daadada",
                 "filters":["namefilter","namefilter2"],
                 "tokenizer": "nametoken"}

        request = rf.post('/api/analyzers/',
                          data=json.dumps(data),
                          content_type="application/json")
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='

        response = AnalyzerView.as_view()(request)
        self.assertEqual(response.status_code, 201)


    def test_analyzer_post_anonymous(self):
        rf1 = self.factory

        # Utilisation du token_legit
        data = {"name": "daadada",
                "filters": ["namefilter", "namefilter2"],
                "tokenizer": "nametoken"}

        request = rf1.post('/api/analyzers/',
                           data=json.dumps(data),
                           content_type="application/json")
        request.META['HTTP_AUTHORIZATION'] = ''

        response = AnalyzerView.as_view()(request)
        self.assertEqual(response.status_code, 401)


    def test_analyzer_post_error_filters(self):
        rf1 = self.factory

        # Utilisation du token_legit mais d'un mauvais filtre
        data = {"name":"daadada",
                 "filters":["namefilter","filter_unknown"],
                 "tokenizer": "nametoken"}

        request = rf1.post('/api/analyzers/',
                          data=json.dumps(data),
                          content_type="application/json")
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='

        response = AnalyzerView.as_view()(request)
        self.assertEqual(response.status_code, 400)


    def test_analyzer_post_error_token(self):
        rf1 = self.factory

        # Utilisation de filtres correct mais d'un mauvais token
        data = {"name": "daadada",
                "filters": ["namefilter", "namefilter2"],
                "tokenizer": "token_uknown"}

        request = rf1.post('/api/analyzers/',
                           data=json.dumps(data),
                           content_type="application/json")
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='

        response = AnalyzerView.as_view()(request)
        self.assertEqual(response.status_code, 400)


    def test_analyzer_id_get(self):
        rf = self.factory
        name = "nameanalyzer"
        request = rf.get('/api/analyzers/{}'.format(name))
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='
        response = AnalyzerIDView.as_view()(request, name=name)
        self.assertEqual(response.status_code, 200)


    def test_analyzer_id_get_anonymous(self):
        rf = self.factory
        name = "nameanalyzer"
        request = rf.get('/api/analyzers/{}'.format(name))
        request.META['HTTP_AUTHORIZATION'] = ''
        response = AnalyzerIDView.as_view()(request, name=name)
        self.assertEqual(response.status_code, 401)


    def test_analyzer_id_delete(self):
        rf = self.factory
        name = "nameanalyzerdelete"
        request = rf.delete('/api/analyzers/{}'.format(name))
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='
        response = AnalyzerIDView.as_view()(request, name=name)
        self.assertEqual(response.status_code, 204)


    def test_analyzer_id_delete(self):
        rf = self.factory
        name = "nameanalyzerdelete"
        request = rf.delete('/api/analyzers/{}'.format(name))
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='
        response = AnalyzerIDView.as_view()(request, name=name)
        self.assertEqual(response.status_code, 204)


    def test_analyzer_id_put(self):
        rf = self.factory

        data = {"name": "daadada",
                "filters": ["namefilter", "namefilter2"],
                "tokenizer": "nametoken"}

        name = "nameanalyzer"
        request = rf.put('/api/analyzers/{}'.format(name),
                          data=json.dumps(data),
                          content_type="application/json")
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='

        response = AnalyzerIDView.as_view()(request, name=name)
        self.assertEqual(response.status_code, 204)

    def test_analyzer_id_put_anonymous(self):
        rf = self.factory

        data = {"name": "daadada",
                "filters": ["namefilter", "namefilter2"],
                "tokenizer": "nametoken"}

        name = "nameanalyzer"
        request = rf.put('/api/analyzers/{}'.format(name),
                          data=json.dumps(data),
                          content_type="application/json")
        request.META['HTTP_AUTHORIZATION'] = ''

        response = AnalyzerIDView.as_view()(request, name=name)
        self.assertEqual(response.status_code, 401)

    def test_analyzer_id_put_error_filter(self):
        rf = self.factory

        data = {"name": "daadada",
                "filters": ["namefilter", "namefilter_unkown"],
                "tokenizer": "nametoken"}

        name = "nameanalyzer"
        request = rf.put('/api/analyzers/{}'.format(name),
                          data=json.dumps(data),
                          content_type="application/json")
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='

        response = AnalyzerIDView.as_view()(request, name=name)
        self.assertEqual(response.status_code, 400)

    def test_analyzer_id_put_error_token(self):
        rf = self.factory

        data = {"name": "daadada",
                "filters": ["namefilter", "namefilter2"],
                "tokenizer": "token_unknown"}

        name = "nameanalyzer"
        request = rf.put('/api/analyzers/{}'.format(name),
                          data=json.dumps(data),
                          content_type="application/json")
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='

        response = AnalyzerIDView.as_view()(request, name=name)
        self.assertEqual(response.status_code, 400)



    ########################
    #       TOKENIZER      #
    ########################
    def test_token_get(self):
        rf1 = self.factory
        request1 = rf1.get('/api/tokenizers')
        request1.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='  # base64(user1:passpass)
        response1 = TokenizerView.as_view()(request1)
        self.assertEqual(response1.status_code, 200)


    def test_token_get_anonymous(self):
        rf1 = self.factory
        request1 = rf1.get('/api/tokenizers')
        request1.META['HTTP_AUTHORIZATION'] = ''
        response1 = TokenizerView.as_view()(request1)
        self.assertEqual(response1.status_code, 401)


    def test_token_post(self):
        re = self.factory

        data = {"config": {"conf1": "val1"},
                "name": "nametoken2"}

        request = re.post('/api/tokenizers/',
                          data=json.dumps(data),
                          content_type="application/json")
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='

        response = TokenizerView.as_view()(request)
        self.assertEqual(response.status_code, 201)


    def test_token_post_anonymous(self):
        re = self.factory

        data = {"config": {"conf1": "val1"},
                "name": "nametoken2"}

        request = re.post('/api/tokenizers/',
                          data=json.dumps(data),
                          content_type="application/json")
        request.META['HTTP_AUTHORIZATION'] = ''

        response = TokenizerView.as_view()(request)
        self.assertEqual(response.status_code, 401)


    def test_token_post_name_double(self):
        re = self.factory

        data = {"config": {"conf1": "val1"},
                "name": "nametoken"}

        request = re.post('/api/tokenizers/',
                                    data=json.dumps(data),
                                    content_type="application/json")
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='

        response = TokenizerView.as_view()(request)
        self.assertEqual(response.status_code, 409)


    def test_token_id_get(self):
        rf = self.factory
        name = "nametoken"
        request = rf.get('/api/tokenizers/{}'.format(name),
                        content_type="application/json")
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='
        response = TokenizerIDView.as_view()(request, name=name)
        self.assertEqual(response.status_code, 200)


    def test_token_id_get_anonymouse(self):
        rf = self.factory
        name = "nametoken"
        request = rf.get('/api/tokenizers/{}'.format(name),
                        content_type="application/json")
        request.META['HTTP_AUTHORIZATION'] = ''
        response = TokenizerIDView.as_view()(request, name=name)
        self.assertEqual(response.status_code, 401)

    def test_token_id_delete(self):
        rf = self.factory
        name = "nametokendelete"
        request = rf.delete('/api/tokenizers/{}'.format(name))
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='
        response = TokenizerIDView.as_view()(request, name=name)
        self.assertEqual(response.status_code, 204)


    def test_token_id_put(self):
        rf = self.factory
        name = "nametoken"
        data = {"config": {"conf1": "val1"}}

        request = rf.put('/api/tokenizers/{}'.format(name),
                         data=json.dumps(data),
                         content_type="application/json")
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='
        response = TokenizerIDView.as_view()(request, name=name)
        self.assertEqual(response.status_code, 204)

    def test_token_id_put_wrong_usr(self):
        rf = self.factory
        name = "nametoken_usr2"
        data = {"config": {"conf1": "val1"}}

        request = rf.put('/api/tokenizers/{}'.format(name),
                         data=json.dumps(data),
                         content_type="application/json")
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='
        response = TokenizerIDView.as_view()(request, name=name)
        self.assertEqual(response.status_code, 403)

    def test_token_id_put_anonymouse(self):
        rf = self.factory
        name = "nametoken"
        data = {"config": {"conf1": "val1"}}

        request = rf.put('/api/tokenizers/{}'.format(name),
                         data=json.dumps(data),
                         content_type="application/json")
        request.META['HTTP_AUTHORIZATION'] = ''
        response = TokenizerIDView.as_view()(request, name=name)
        self.assertEqual(response.status_code, 401)


    # def test_action_view_post(self):
    #     rf = RequestFactory()
    #     src = Source.objects.get(user=self.user, name=self.name_source_legit)
    #     rsrc = Resource.objects.filter(source=src)
    #     ppt_list = [{"name": "file", "type": "pdf", "occurs": [1, 1]},
    #                 {"name": "/LOCALISATION", "type": None, "occurs": [0, 1]},
    #                 {"name": "/DOCUMENT_REF", "type": None, "occurs": [0, 1]},
    #                 {"name": "/DOSSIER_REF", "type": None, "occurs": [0, 1]},
    #                 {"name": "/Title", "type": None, "occurs": [0, 1]},
    #                 {"name": "/CreationDate", "type": None, "occurs": [0, 1]},
    #                 {"name": "/DateSeance", "type": None, "occurs": [0, 1]}]
    #     Context.objects.create(resource=rsrc[0], name="ctx_name3", clmn_properties=ppt_list)
    #     action_post_data = {"index": "ctx_name3",
    #                         "type": "rebuild"
    #                         }
    #     json_str = json.dumps(action_post_data)
    #     request = rf.post('/api/action/',
    #                                 data=json_str,
    #                                 content_type="application/json")
    #     request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='
    #
    #     response = ActionView.as_view()(request)
    #     self.assertEqual(response.status_code, 202)
    #
    # def test_search_model_post(self):
    #     rf = RequestFactory()
    #     model_post_data = {"config":{"daadada":"dedededed"},
    #                        "contexts":["namecontext"],
    #                        "name":"modelname"
    #                        }
    #     json_str = json.dumps(model_post_data)
    #     request = rf.post('/api/models/',
    #                                 data=json_str,
    #                                 content_type="application/json")
    #     request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='
    #     response = SearchModelView.as_view()(request)
    #     self.assertEqual(response.status_code, 201)
    #
    #
    #     # Creation context avec le meme nom que le model
    #     src = Source.objects.get(user=self.user, name=self.name_source_legit)
    #     rsrc = Resource.objects.filter(source=src)
    #     ctx_post_data = {"name": "modelname",
    #                      "resource": "/sources/{}/resources/{}".format(src.id, rsrc[0].id),
    #                      "reindex_frequency": "daily"
    #                      }
    #     json_str = json.dumps(ctx_post_data)
    #     request = rf.post('/api/contexts/',
    #                                 data=json_str,
    #                                 content_type="application/json")
    #     request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='
    #
    #     response = ContextView.as_view()(request)
    #     self.assertEqual(response.status_code, 409)
    #
    #
    # def test_search_view_post(self):
    #     re = RequestFactory()
    #
    #     # context
    #     src = Source.objects.get(user=self.user, name=self.name_source_legit)
    #     rsrc = Resource.objects.filter(source=src)
    #
    #     ctx_post_data = {"name": "ctxname",
    #                      "resource": "/sources/{}/resources/{}".format(src.id, rsrc[0].id),
    #                      "reindex_frequency": "daily"
    #                      }
    #     request = re.post('/api/contexts/',
    #                         data=json.dumps(ctx_post_data),
    #                         content_type="application/json")
    #     request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='
    #     response = ContextView.as_view()(request)
    #     self.assertEqual(response.status_code, 201)
    #
    #
    #     # search model
    #     model_post_data = {"config": {"daadada": "dedededed"},
    #                        "contexts": ["ctxname"],
    #                        "name": "modelname"
    #                        }
    #     request2 = re.post('/api/models/',
    #                       data=json.dumps(model_post_data),
    #                       content_type="application/json")
    #     request2.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='
    #     response2 = SearchModelView.as_view()(request2)
    #     self.assertEqual(response2.status_code, 201)
    #
    #
    #     # Search:
    #     sm = SearchModel.objects.get(name="modelname")
    #
    #     ## request sans mode == 501
    #     request3 = re.post('/api/models/{}/search/'.format(sm.name),
    #                       {"mode" : ""},
    #                       content_type="application/json")
    #     request3.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='
    #     response3 = SearchView.as_view()(request3, name=(str(sm.name)))
    #     self.assertEqual(response3.status_code, 501)

        ## request avec mode -- a completer avec données elastic search pour data
        # request4 = re.post('/api/models/{}/search/'.format(sm.name),
        #                    {"mode": "throw"},
        #                    content_type="application/json")
        # request4.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjE6cGFzc3Bhc3M='
        # response4 = SearchView.as_view()(request4, name=(str(sm.name)))
        # self.assertEqual(response4.status_code, 200)

