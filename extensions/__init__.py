from abc import ABCMeta, abstractmethod
from django.http import JsonResponse
from functools import partial, wraps
from json import dumps, loads
from re import sub


def input_parser(f):

    @wraps(f)
    def wrapper(self, config, **params):
        config = dumps(config)

        for k, v in params.items():
            config = config.replace('{{%{0}%}}'.format(k), v)

        config = sub('\"\{\%\s*((\d+\s*[-+\*/]?\s*)+)\%\}\"',
                     partial(lambda m: str(eval(m.group(1)))), config)

        return f(self, loads(config), **params)
    return wrapper


class AbstractPlugin(metaclass=ABCMeta):

    @abstractmethod
    def input(self, config, **params):
        raise NotImplementedError('This is an abstract method. '
                                  "You can't do anything with it.")

    @abstractmethod
    def output(self, data, **params):
        raise NotImplementedError('This is an abstract method. '
                                  "You can't do anything with it.")


class Plugin(AbstractPlugin):

    @input_parser
    def input(self, config, **params):
        if not config or not params:
            return {'query': {'match_all': {}}}
        return config

    def output(self, data, **params):

        results = []
        for hit in data['hits']['hits']:
            d1 = {'id': '_id' in hit and hit['_id'] or None,
                  'score': '_score' in hit and hit['_score'] or None,
                  'index': '_type' in hit and hit['_type'] or None}

            if 'highlight' in hit and hit['highlight']:
                d1['highlight'] = hit['highlight']

            d2 = dict((k, v) for k, v in hit['_source'].items())

            results.append({**d1, **d2})

        response = {'results': results,
                    'total': data['hits']['total']}

        if 'aggregations' in data:
            response['aggregations'] = data['aggregations']

        return JsonResponse(response)


plugin = Plugin
