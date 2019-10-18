# Copyright (c) 2017-2018 Neogeo-Technologies.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.


from django.contrib.auth.models import User
from django.contrib.postgres.fields import JSONField
from django.core.exceptions import ValidationError
from django.db import models
from functools import reduce
from onegeo_api.elastic import elastic_conn
from onegeo_api.exceptions import ConflictError
from onegeo_api.exceptions import ElasticError
from onegeo_api.utils import merge_two_objs
from uuid import uuid4


class Analysis(models.Model):

    class Meta(object):
        verbose_name = 'Analysis document'
        verbose_name_plural = 'Analysis documents'

    title = models.TextField(verbose_name='Title')

    user = models.ForeignKey(User, verbose_name='User', on_delete=models.CASCADE)

    document = JSONField(verbose_name='Document')

    def clean(self, *args, **kwargs):

        if not self.user_id:
            raise ValidationError('User is mandatory.')  # C'est caca

        components = self.get_components(
            user=self.user, exclude_pk=self.pk and [self.pk])

        # Vérifie si le document ne contient pas des composants du même nom..
        for component in components.keys():
            key = component[:-1]  # plural to singular
            if key in self.document:
                for val in self.document[key].keys():
                    if val in components[component]:
                        raise ValidationError(
                            "The {0} name '{1}' is already taken.".format(key, val))

        # Vérifie si le document ne contient pas de doublon ambigu..
        # Par exemple deux composants de même nom (c'est le cas pour les
        # « filters » et « token filters ») doivent être strictement identique
        # afin de ne pas génèrer des incohérences lors de la compilation
        # du « settings ».
        try:
            reduce(merge_two_objs, [
                instance.document for instance
                in Analysis.objects.filter(user=self.user)] + [self.document])
        except ConflictError as e:
            raise ValidationError(e.__str__())

        # Vérifie si le document est valide..
        try:
            index = str(uuid4())
            elastic_conn.create_index(
                index, {'settings': {'analysis': self.document}})
        except ElasticError as e:
            raise ValidationError(e.description)
        else:
            elastic_conn.delete_index(index)

    @classmethod
    def get_components(cls, user=None, exclude_pk=None):
        data = {'analyzers': [], 'normalizers': []}
        queryset = cls.objects.filter(user=user)
        if exclude_pk:
            queryset = queryset.exclude(pk__in=exclude_pk)
        for instance in queryset:
            document = instance.document
            for component in data.keys():
                key = component[:-1]  # plural to singular
                if key in document:
                    data[component].extend(list(document[key].keys()))
        return data

    @classmethod
    def get_component_by_name(cls, component, name, user=None):
        for instance in cls.objects.filter(user=user):
            if component in instance.document \
                    and name in instance.document[component]:
                return instance.document
        # else:
        raise cls.DoesNotExist(
            "Analysis component '{0}' as {1} does not exists.".format(
                name, component))


def get_complete_analysis(user=None, **kwargs):
    documents = []
    for component, names in kwargs.items():
        for name in names:
            documents.append(
                Analysis.get_component_by_name(component, name, user=user))
    if len(documents) > 1:
        return reduce(merge_two_objs, documents)
    if len(documents) == 1:
        return documents[0]
    return {}
