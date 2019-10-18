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


from django.apps import apps
from django.contrib.auth.models import User
from django.contrib.postgres.fields import JSONField
from django.core.exceptions import PermissionDenied
from django.db import models
from django.http import Http404
from django.urls import reverse
from django.utils import timezone
from onegeo_api.utils import pagination_handler
from uuid import uuid4


APP = 'onegeo_api'


class AsyncTaskManager(models.Manager):

    def create(self, **kwargs):
        kwargs['is_async'] = True
        super().create(**kwargs)

    def get_queryset(self):
        return super().get_queryset().filter(is_async=True)


class Task(models.Model):

    class Meta(object):
        verbose_name = 'Task'
        verbose_name_plural = 'Tasks'

    uuid = models.UUIDField(
        verbose_name='UUID', default=uuid4, primary_key=True, editable=False)

    alias = models.ForeignKey(
        to='Alias', verbose_name='Alias', on_delete=models.CASCADE)

    resource_ns = models.CharField(
        verbose_name='Resource Namespace', max_length=100)

    user = models.ForeignKey(to=User, verbose_name='User',
                             on_delete=models.CASCADE)

    task_name = models.CharField(
        verbose_name='Task name', max_length=100, null=True, blank=True)

    is_async = models.BooleanField(
        verbose_name='Is asynchronous', default=False)

    success = models.NullBooleanField(verbose_name='Success')

    start_date = models.DateTimeField(
        verbose_name='Start', auto_now_add=True)

    stop_date = models.DateTimeField(
        verbose_name='Stop', null=True, blank=True)

    details = JSONField(verbose_name='Details', blank=True, null=True)

    logged = models.Manager()
    asynchronous = AsyncTaskManager()

    @property
    def elapsed_time(self):
        return self.stop_date \
            and (self.stop_date - self.start_date) \
            or (timezone.now() - self.start_date)

    @elapsed_time.setter
    def elapsed_time(self, *args, **kwargs):
        raise AttributeError('Attibute is locked, you can not change it.')

    @elapsed_time.deleter
    def elapsed_time(self, *args, **kwargs):
        raise AttributeError('Attibute is locked, you can not delete it.')

    @property
    def location(self):
        return reverse(
            'onegeo_api:logged_task', kwargs={'uuid': str(self.uuid)})

    @location.setter
    def location(self, *args, **kwargs):
        raise AttributeError('Attibute is locked, you can not change it.')

    @location.deleter
    def location(self, *args, **kwargs):
        raise AttributeError('Attibute is locked, you can not delete it.')

    @property
    def target_location(self):
        Model = apps.get_model(APP, self.alias.model_name)
        instance = Model.objects.get(alias=self.alias)
        return reverse(
            'onegeo_api:{}'.format(self.resource_ns),
            kwargs={'name': str(instance.name)})

    @target_location.setter
    def target_location(self, *args, **kwargs):
        raise AttributeError('Attibute is locked, you can not change it.')

    @target_location.deleter
    def target_location(self, *args, **kwargs):
        raise AttributeError('Attibute is locked, you can not delete it.')

    def detail_renderer(self):

        return {
            'task_name': self.task_name,
            'dates': {
                'start': self.start_date,
                'end': self.stop_date},
            'description': self.details,
            'elapsed_time': float(
                '{0:.2f}'.format(self.elapsed_time.total_seconds())),
            'id': self.uuid,
            'location': self.location,
            'status': {
                None: 'pending',
                False: 'failed',
                True: 'done'}.get(self.success),
            'target': self.target_location}

    @classmethod
    @pagination_handler
    def list_renderer(cls, defaults, i=0, j=None, **kwargs):
        return [t.detail_renderer() for t in
                cls.logged.filter(**defaults).order_by('-start_date')[i:j]]

    @classmethod
    def get_with_permission(cls, defaults, user):
        try:
            instance = cls.logged.get(**defaults)
        except cls.DoesNotExist:
            raise Http404()
        if instance.user and instance.user != user:
            raise PermissionDenied()
        return instance
