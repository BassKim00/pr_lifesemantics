from django.db import models

class Member(models.Modes):
	name=models.CharField(max_length=200)
	mail=models.CharField(max_length=200)
	age=models.IntegerField(default=0)