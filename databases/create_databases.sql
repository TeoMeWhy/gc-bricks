-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS bronze_gc LOCATION '/mnt/datalake/bronze/gc/';
CREATE SCHEMA IF NOT EXISTS silver_gc LOCATION '/mnt/datalake/silver/gc/';
CREATE SCHEMA IF NOT EXISTS silver_gc_fs LOCATION '/mnt/datalake/silver/gc_fs/';
