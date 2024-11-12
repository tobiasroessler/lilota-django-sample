import os
os.environ['DJANGO_SETTINGS_MODULE'] = 'mysite.settings'

import django
django.setup()

from unittest import TestCase, main
from multiprocessing import cpu_count
from lilota.runner import TaskRunner
from lilota.stores import StoreManager
from lilota.models import TaskBase
from lilota_django.stores import DjangoTaskStore
from lilota_django.models import Task
import logging


class AdditionTask(TaskBase):
  
  def run(self):
    output = {
      "result": self.task_info.input["number1"] + self.task_info.input["number2"]
    }
    self.set_output(self.task_info.id, output)


class DjangoTaskRunnerTestCase(TestCase):

  @classmethod
  def setUpTestData(cls):
    pass


  def setUp(self):
    Task.objects.all().delete()


  def test_add___add_5000_tasks___should_calculate_the_results(self):
    # Arrange
    store, store_manager, runner = self.create_task_runner(1)
    runner.register("add_task", AdditionTask)
    runner.start()

    # Act
    for i in range(1, 5001):
      runner.add("add_task", "Add two numbers", {"number1": i, "number2": i})

    # Assert
    runner.stop()
    tasks = store.get_all_tasks()
    self.assertIsNotNone(tasks)
    self.assertEqual(len(tasks), 5000)

    for task in tasks:
      number1 = task.input["number1"]
      number2 = task.input["number2"]
      result = task.output["result"]
      self.assertEqual(number1 + number2, result)
    
    store_manager.shutdown()


  def create_task_runner(self, number_of_processes: int = cpu_count()):
    StoreManager.register("Store", DjangoTaskStore)
    store_manager = StoreManager()
    store_manager.start()
    store = store_manager.Store()
    runner = TaskRunner(store, number_of_processes, logging_level=logging.INFO)
    return (store, store_manager, runner)
  
if __name__ == '__main__':
  main()