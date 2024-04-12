# Sanções

O dataset de sanções é composto por 4 bases disponíveis no Portal da Transparência:
- Empresas Inidôneas e Suspensas (CEIS)
- Entidades sem Fins Lucrativos Impedidas (CEPIM)
- Empresas Punidas (CNEP)
- Acordos de Leniência

Os CSVs disponíveis nesses datasets não são exatamente iguais, então criamos um processo de normalização para que eles
fiquem com a mesma estrutura. O arquivo que geramos depois desse processo de normalização está em anexo e possui as
seguintes colunas:

- `object_uuid`: URLid do objeto sancionado
- `nome`: nome ou razão social do sancionado
- `documento`: CPF ou CNPJ do sancionado
- `processo`: número do processo
- `tipo`: tipo de sanção (são 4 possíveis: Inidônea/Suspensa, Impedida de licitar, Punida e Acordo de leniência)
- `detalhe_tipo`: mais informações (quando disponíveis - não disponível para CEPIM) sobre a sanção (exemplo: no caso de Acordo de leniência pode ser "Cumprido" ou "Em execução")
- `data_inicio`: data inicial da sanção (se disponível - não disponível para CEPIM)
- `data_final`: data final da sanção (se disponível - não disponível para CEPIM)
- `orgao`: órgão responsável por aplicar a sanção
- `fundamentacao`: texto com a fundamentação (se disponível - não disponível para acordo de leniência)
- `multa`: multa aplicada em R$ (se disponível - só disponível para CNEP)
