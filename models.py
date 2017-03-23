from pathlib import Path
from re import search

from django.contrib.auth.models import User
from django.db import models
from django.contrib.postgres.fields import JSONField
from django.conf import settings
from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver
from onegeo_manager.source import PdfSource
from django.core.exceptions import ValidationError
# from onegeo_manager.index import Index
# from onegeo_manager.context import PdfContext

from .elasticsearch_wrapper import elastic_conn


PDF_BASE_DIR = settings.PDF_DATA_BASE_DIR


def retrieve(b):
    p = Path(b.startswith("file://") and b[7:] or b)

    if not p.exists():
        raise ConnectionError('Given path does not exist.')

    return [x.as_uri() for x in p.iterdir() if x.is_dir()]


def does_uri_exist(uri):
    p = Path(uri.startswith("file://") and uri[7:] or uri)
    if not p.exists():
        return False
    if p.as_uri() in retrieve(PDF_BASE_DIR):
        return True


class Source(models.Model):

    MODE_L = (("pdf","pdf"),)
    user = models.ForeignKey(User)
    uri = models.CharField("URI", max_length=2048, unique=True)
    name = models.CharField("Name", max_length=250)
    mode = models.CharField("Mode", choices= MODE_L, default="pdf", max_length=250)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__src = None


    def save(self, *args, **kwargs):

        if not does_uri_exist(self.uri):
            raise Exception()  # TODO

        self.__src = PdfSource(self.uri, self.name, self.mode)

        super().save(*args, **kwargs)

    @property
    def src(self):
        return self.__src

    class Meta:
        verbose_name = "Source"
        unique_together = (('uri', 'user'),)

    @property
    def s_uri(self):
        dir_name = search('(\S+)\/(\S+)', self.uri)
        return 'file:///{}'.format(dir_name.group(2))


class Resource(models.Model):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__rsrc = None

    source = models.ForeignKey(Source, on_delete=models.CASCADE)
    name = models.CharField("Name", max_length=250)
    columns = JSONField("Columns")

    class Meta:
        verbose_name = "Resource"

    @property
    def rsrc(self):
        return self.__rsrc

    def set_rsrc(self, rsrc):
        self.__rsrc = rsrc


class Context(models.Model):
    
    RF_L = (
        ("daily", "daily"),
        ("weekly", "weekly"),
        ("monthly", "monthly"),)

    resource = models.OneToOneField(Resource, on_delete=models.CASCADE, primary_key=True)
    name = models.CharField("Name", max_length=250, unique=True)
    clmn_properties = JSONField("Columns")
    reindex_frequency = models.CharField("Reindex_frequency", choices=RF_L, default="monthly", max_length=250)

    def save(self, *args, **kwargs):
        set_names_sm = SearchModel.objects.all()
        for s in set_names_sm:
            if self.name == s.name:
                raise ValidationError("Un context ne peut avoir le même nom qu'un model de recherche")
        super().save(*args, **kwargs)


class Filter(models.Model):

    name = models.CharField("Name", max_length=250, unique=True, primary_key=True)
    user = models.ForeignKey(User, blank=True, null=True)
    config = JSONField("Config", blank=True, null=True)
    reserved = models.BooleanField("Reserved", default=False)


class Analyzer(models.Model):

    name = models.CharField("Name", max_length=250, unique=True, primary_key=True)
    user = models.ForeignKey(User, blank=True, null=True)
    filter = models.ManyToManyField(Filter, blank=True)
    tokenizer = models.ForeignKey("Tokenizer", blank=True, null=True)
    reserved = models.BooleanField("Reserved", default=False)


class Tokenizer(models.Model):

    name = models.CharField("Name", max_length=250, unique=True, primary_key=True)
    user = models.ForeignKey(User, blank=True, null=True)
    config = JSONField("Config", blank=True, null=True)
    reserved = models.BooleanField("Reserved", default=False)

class SearchModel(models.Model):

    user = models.ForeignKey(User, blank=True, null=True)
    name = models.CharField("Name", max_length=250, unique=True, primary_key=True)
    context = models.ManyToManyField(Context, blank=True)
    config = JSONField("Config", blank=True, null=True)

    def save(self, *args, **kwargs):
        set_names_ctx = Context.objects.all()
        for c in set_names_ctx:
            if self.name == c.name:
                raise ValidationError("Un model de recherche ne peut avoir le même nom qu'un context")

        super().save(*args, **kwargs)

class Task(models.Model):

    desc = models.CharField("Nom de l'opération", max_length=256)
    is_running = models.BooleanField("Tâche en cours", default=True)
    start_date = models.DateTimeField('Start', null=True, blank=True)
    stop_date = models.DateTimeField('Stop', null=True, blank=True)
    success = models.BooleanField("Success", default=True)


@receiver(post_save, sender=Source)
def on_save_source(sender, instance, *args, **kwargs):
    for res in instance.src.get_types():
        resource = Resource.objects.create(source=instance, name=res.name, columns=res.columns)
        resource.set_rsrc(res)


@receiver(post_delete, sender=Context)
def on_delete_context(sender, instance, *args, **kwargs):
    elastic_conn.delete_index_by_alias(instance.name)
