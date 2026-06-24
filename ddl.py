# Databricks notebook source
spark.sql("create schema if not exists workspace.project")
spark.sql("create volume if not exists project.manual_file_project")

print("schema and volume are created successfully")