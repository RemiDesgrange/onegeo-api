from django.contrib.auth.models import User
from django.contrib.postgres.fields import JSONField
from django.db import models
# from django.core.exceptions import ObjectDoesNotExist
# from django.core.exceptions import ValidationError

from django.db.models.signals import post_delete
from django.dispatch import receiver

from onegeo_api.utils import clean_my_obj

import uuid


class Alias(models.Model):
    """
        table d'alias utiliser sur l'ensemble des model
    """
    MODELS_CHOICES = (
        ('Analyzer', 'Analyzer'),
        ('Context', 'Context'),
        ('Filter', 'Filter'),
        ('Resource', 'Resource'),
        ('SearchModel', 'SearchModel'),
        ('Source', 'Source'),
        ('Tokenizer', 'Tokenizer'),
        ('Undefined', 'Undefined'),
        )
    uuid = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    handle = models.CharField("Alias", max_length=250, unique=True)
    model_name = models.CharField(max_length=30, choices=MODELS_CHOICES, default='Undefined')

    def save(self, *args, **kwargs):
        # Si creation sans alias depuis les modeles.
        if not self.handle:
            self.handle = str(self.uuid)
        return super().save(*args, **kwargs)

    @classmethod
    def custom_creator(cls, model_name, handle=None):
        return cls.objects.create(**clean_my_obj({"model_name": model_name, "handle": handle}))

    def custom_updater(self, custom_handle):
        self.handle = custom_handle
        self.save()

    @classmethod
    def updating_is_allowed(cls, new_handle, current_handle):
        if new_handle != current_handle:
            if cls.objects.filter(handle=new_handle).exists():
                return False
        return True


class AbstractModelAnalyzis(models.Model):
    """
        Héritée par modeles: Analyzer, Filter, Tokenizer
    """

    name = models.CharField("Name", max_length=250, unique=True)
    user = models.ForeignKey(User, blank=True, null=True)
    config = JSONField("Config", blank=True, null=True)
    reserved = models.BooleanField("Reserved", default=False)

    alias = models.OneToOneField("Alias", on_delete=models.CASCADE)

    class Meta:
        abstract = True

    def __unicode__(self):
            return self.name

    def save(self, *args, **kwargs):
        model_name = kwargs.pop('model_name', 'Undefined')

        # "if not self.alias" retourne une erreur RelatedObjectDoesNotExist ??
        if not self.alias_id:
            self.alias = Alias.objects.create(model_name=model_name)
        return super().save(*args, **kwargs)

    @property
    def detail_renderer(self):
        raise NotImplemented

    @classmethod
    def list_renderer(cls, *args, **kwargs):
        raise NotImplemented

    @classmethod
    def create_with_response(cls, *args, **kwargs):
        raise NotImplemented

    @property
    def delete_with_response(self):
        raise NotImplemented

    @classmethod
    def custom_filter(cls, *args, **kwargs):
        raise NotImplemented

    @classmethod
    def get_from_name(cls, *args, **kwargs):
        raise NotImplemented

    @classmethod
    def get_with_permission(cls, *args, **kwargs):
        raise NotImplemented


class AbstractModelProfile(models.Model):

    name = models.CharField("Name", max_length=250, unique=True)
    user = models.ForeignKey(User, blank=True, null=True)

    alias = models.OneToOneField("Alias", on_delete=models.CASCADE)

    class Meta:
        abstract = True

    def __unicode__(self):
        return self.name

    def save(self, *args, **kwargs):
        model_name = kwargs.pop('model_name', 'Undefined')

        # "if not self.alias" retourne une erreur RelatedObjectDoesNotExist ??
        if not self.alias_id:
            self.alias = Alias.objects.create(model_name=model_name)
        return super().save(*args, **kwargs)

    @property
    def detail_renderer(self):
        raise NotImplemented

    @classmethod
    def list_renderer(cls, *args, **kwargs):
        raise NotImplemented

    @classmethod
    def create_with_response(cls, *args, **kwargs):
        raise NotImplemented

    @classmethod
    def get_from_uuid(cls, *args, **kwargs):
        raise NotImplemented

    @classmethod
    def get_from_name(cls, *args, **kwargs):
        raise NotImplemented

    @classmethod
    def get_with_permission(cls, *args, **kwargs):
        raise NotImplemented
