MeldRx raw layer - test kit (DATA-200)

Follow MeldRx_Raw_Layer_Test_Setup_Guide.pdf. Contents:
  app/meldrx_ingest.py      reference ingest (split, landing, Glue tables/partitions, deletions, quarantine)
  app/run_job.sh            container entrypoint: ingest, then the downstream command
  app/post_check.py         downstream step for the test (Athena queries); stands in for `dbt build`
  app/Dockerfile            image used by the ECS task
  make_test_packages.py     writes FAKE test packages (python3 make_test_packages.py testdata)
  commands.sh               every command from the guide, in order (run section by section)
  mock_test_sequence.py     optional offline check with mocked AWS:
                              pip install "moto[s3,glue]" boto3
                              python3 make_test_packages.py testdata && python3 mock_test_sequence.py
