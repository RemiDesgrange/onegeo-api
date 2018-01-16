from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver
from django.apps import apps
from django.utils import timezone

from onegeo_api.elasticsearch_wrapper import elastic_conn
from onegeo_api.models.analyzis import Analyzer
from onegeo_api.models.analyzis import Filter
from onegeo_api.models.analyzis import Tokenizer
from onegeo_api.models.context import Context
from onegeo_api.models.resource import Resource
from onegeo_api.models.search_model import SearchModel
from onegeo_api.models.source import Source


#Ces connecteurs de signaux ont été enregistré dans les modules apps.py et __init__.py de l'application

@receiver(post_delete, sender=Analyzer)
@receiver(post_delete, sender=Context)
@receiver(post_delete, sender=Filter)
@receiver(post_delete, sender=Resource)
@receiver(post_delete, sender=SearchModel)
@receiver(post_delete, sender=Source)
@receiver(post_delete, sender=Tokenizer)
def delete_related_alias(sender, instance, **kwargs):
    if instance.alias:
        instance.alias.delete()


@receiver(post_save, sender=Source)
def on_post_save_source(sender, instance, *args, **kwargs):
    Task = apps.get_model(app_label='onegeo_api', model_name='Task')
    Resource = apps.get_model(app_label='onegeo_api', model_name='Resource')

    def create_resources(instance, tsk):
        try:
            for res in instance.src.get_resources():
                # resource = Resource.custom_create(instance, res.name, res.columns, instance.user)
                resource = Resource.objects.create(
                    source=instance, name=res.name,
                    columns=res.columns, user=instance.user)
                resource.set_rsrc(res)
            tsk.success = True
            tsk.description = "Les ressources ont été créées avec succès. "
        except Exception as err:
            tsk.success = False
            tsk.description = str(err)  # TODO
        finally:
            tsk.stop_date = timezone.now()
            tsk.save()

    description = ("Création des ressources en cours. "
                   "Cette opération peut prendre plusieurs minutes. ")

    tsk = Task.objects.create(
        model_type="source", user=instance.user,
        model_type_alias=instance.alias.handle, description=description)
    create_resources(instance, tsk)
    # TODO: Mis en echec des test lors de l'utilisation de thread
    # thread = Thread(target=create_resources, args=(instance, tsk))
    # thread.start()


@receiver(post_delete, sender=Context)
def on_delete_context(sender, instance, *args, **kwargs):
    Task = apps.get_model(app_label='onegeo_api', model_name='Task')
    Task.objects.filter(model_type_alias=instance.alias.handle, model_type="context").delete()
    # elastic_conn.delete_index_by_alias(instance.name) #Erreur sur l'attribut indices à None
