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
from django.contrib.sites.shortcuts import get_current_site
from django.core.exceptions import ValidationError
from django.db import models
from django.urls import reverse
from onegeo_api.extensions import DEFAULT_QUERY_DSL
from onegeo_api.models.abstracts import AbstractModelProfile
import re


class SearchModel(AbstractModelProfile):

    class Extras(object):
        fields = ('indexes', 'location', 'query_dsl', 'title')

    class Meta(object):
        verbose_name = 'Search Model'
        verbose_name_plural = 'Search Models'

    query_dsl = JSONField(
        verbose_name='Query DSL', blank=True, null=True)

    user = models.ForeignKey(
        to=User, verbose_name='User', blank=True, null=True, on_delete=models.CASCADE)

    indexes = models.ManyToManyField(
        to='IndexProfile', verbose_name='Indexation profiles')

    @property
    def location(self):
        return reverse(
            'onegeo_api:search_model', kwargs={'name': str(self.name)})

    @location.setter
    def location(self, value):
        try:
            self.nickname = re.search(
                'services/(?P<name>(\w|-){1,100})/?$', value).group('name')
        except AttributeError:
            raise AttributeError("'Location' attibute is malformed.")

    @location.deleter
    def location(self):
        raise AttributeError('Attibute is locked, you can not delete it.')

    @property
    def service_url(self, with_params=False):
        return 'http://{0}{1}'.format(
            get_current_site(None),
            reverse('onegeo_api:search', kwargs={'name': self.name}))

    @service_url.setter
    def service_url(self, value):
        raise AttributeError('Attibute is locked, you can not change it.')

    @service_url.deleter
    def service_url(self):
        raise AttributeError('Attibute is locked, you can not delete it.')

    def detail_renderer(self, include=False, cascading=False, **kwargs):
        opts = {'include': cascading and include, 'cascading': cascading}
        return {
            'query_dsl': self.query_dsl,
            'indexes': [
                m.detail_renderer(**opts) if include else m.location
                for m in self.indexes.all()],
            'location': self.location,
            'title': self.title,
            'service_url': self.service_url,
            'uuid': self.uuid}

    @classmethod
    def list_renderer(cls, user, **opts):
        return [
            search_model.detail_renderer(**opts)
            for search_model in cls.objects.filter(user=user).order_by('title')]

    def save(self, *args, **kwargs):

        if not self.title:
            raise ValidationError(
                'Some of the input paramaters needed are missing.')

        if not self.query_dsl:
            self.query_dsl = DEFAULT_QUERY_DSL

        super().save(*args, **kwargs)
