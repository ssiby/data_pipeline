Bonjour

Le projet contient des functions python dans pipeline/data_pipeline_functions.py
- data_pipeline_functions.py
  - get_drug_relation_tree : genere un fichier json contenant les drugs et leurs relations et le stocke dans un blob container Azure
  - get_best_journal : retrouve le journal qui a fait le plus de publication sans stocker le resultat dans un blob container
- infra:
  - contient le template des deux storage account : 1 pour le stockage des fichiers de données business et l'autre pour le stockage de la fonction durable azure.
- test :
  Devrait contenir des tests
- durable-functions:
  Contient des functions durables azure:
  - DurableFuntionsHttpStart: fonction d'entrée http
  - DurableDataPipelineOrchestrator: function d'orchestration des functions d'activités
  - DrugTreeDurableActivity: function d'activité qui permet de contruire les relations entre drugs, publication et journal
  - BestJournalDurableActivity: function d'activité qui permet de retrouver le meilleur journal

  Remarques / suggestions :
  1 - Pourquoi développer une API rest et déployer dans azure ACI si Azure durable function propose une entrée http et retour http ?
  2 - Pour les functions d'activité azure, on pourrait prendre le retour de la DrugTreeDurableActivity comme entrée de la function BestJournalDurableActivity pour retouver le meilleur journal
  3 - Coté DevOps / SRE : J'aurai provisionner les resources Azure avec un projet terraform ou Azure ARM ou Bicep
  
