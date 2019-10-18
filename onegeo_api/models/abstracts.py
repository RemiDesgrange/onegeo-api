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


from abc import abstractmethod
from abc import abstractproperty
from django.apps import apps
from django.contrib.auth.models import User
from django.core.exceptions import PermissionDenied
from django.core.exceptions import ValidationError
from django.db import IntegrityError
from django.db import models
from django.http import Http404
import inspect
from uuid import uuid4


class Alias(models.Model):

    uuid = models.UUIDField(
        verbose_name='UUID', default=uuid4, primary_key=True, editable=False)

    alias_name = models.CharField(
        verbose_name='Alias name', max_length=100, blank=True, null=True)

    model_name = models.CharField(verbose_name='Model name', max_length=100)

    def __str__(self):
        return self.alias_name or str(self.uuid)

    def save(self, *args, **kwargs):
        if self.alias_name:
            if self.alias_name.startswith('_'):
                raise ValidationError(
                    "Alias name could not starts with '_' character.")
            queryset = Alias.objects.filter(
                alias_name=self.alias_name, model_name=self.model_name)
            if len(queryset) > 0 and queryset[0] != self:
                raise IntegrityError('This alias name is already in use.')
        super().save(*args, **kwargs)


class AbstractModelProfile(models.Model):

    alias = models.OneToOneField(
        to='Alias', verbose_name='Alias', on_delete=models.CASCADE)

    title = models.TextField(verbose_name='Tilte', blank=True, null=True)

    description = models.TextField(
        verbose_name='Description', blank=True, null=True)

    user = models.ForeignKey(to=User, blank=True, null=True, on_delete=models.CASCADE)

    class Meta(object):
        abstract = True

    def __str__(self):
        return self.name

    def __unicode__(self):
        return self.name

    def __init__(self, *args, **kwargs):
        self.nickname = None
        super().__init__(*args, **kwargs)

    @property
    def name(self):
        return self.alias.alias_name or self.uuid[:7]

    @name.setter
    def name(self, value):
        raise AttributeError("Attibute 'name' is locked, you can not change it.")

    @name.deleter
    def name(self):
        raise AttributeError("Attibute 'name' is locked, you can not delete it.")

    @property
    def uuid(self):
        return str(self.alias.uuid)

    @uuid.setter
    def uuid(self, value):
        if value != str(self.alias.uuid):
            raise AttributeError("Attibute 'uuid' is locked, you can not change it.")

    @uuid.deleter
    def uuid(self):
        raise AttributeError("Attibute 'uuid' is locked, you can not delete it.")

    def save(self, *args, **kwargs):
        try:
            self.alias.alias_name = self.nickname
        except Exception as e:
            if e.__class__.__qualname__ == 'RelatedObjectDoesNotExist':
                stack = inspect.stack()
                caller = stack[0][0].f_locals['self'].__class__.__qualname__
                self.alias = Alias.objects.create(
                    alias_name=self.nickname, model_name=caller)
            else:
                raise e
        else:
            self.alias.save()
        return super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        Task = apps.get_model(app_label='onegeo_api', model_name='Task')
        Task.logged.filter(alias=self.alias).delete()
        return super().delete(*args, **kwargs)

    @abstractproperty
    def location(self):
        raise NotImplementedError(
            "This is an abstract property. You can't do anything with it.")

    @abstractproperty
    def detail_renderer(self):
        raise NotImplementedError(
            "This is an abstract property. You can't do anything with it.")

    @classmethod
    @abstractmethod
    def list_renderer(cls, *args, **kwargs):
        raise NotImplementedError(
            "This is an abstract method. You can't do anything with it.")

    @classmethod
    def get_or_raise(cls, name, user=None):
        queryset = cls.objects.filter(
            models.Q(alias__uuid__startswith=name) |
            models.Q(alias__alias_name=name))
        if len(queryset) != 1:
            raise Http404()
        instance = queryset[0]
        if user and user != instance.user:
            raise PermissionDenied()
        return instance

    @classmethod
    def get_by_location(cls, location, **kwargs):
        queryset = cls.objects.all()
        for instance in queryset:
            if instance.location == location:
                return instance
