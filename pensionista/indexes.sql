CREATE INDEX idx_pensobs_id ON pensionista_observacao (id_servidor_portal, ano, mes, sistema_origem);
CREATE INDEX idx_pensobs_uuid ON pensionista_observacao (pessoa_uuid);
CREATE INDEX idx_pensobs_orig ON pensionista_observacao (sistema_origem);

CREATE INDEX idx_penscad_id ON pensionista_cadastro (id_servidor_portal, ano, mes, sistema_origem);
CREATE INDEX idx_penscad_uuid1 ON pensionista_cadastro (pessoa_uuid);
CREATE INDEX idx_penscad_uuid2 ON pensionista_cadastro (representante_legal_uuid);
CREATE INDEX idx_penscad_uuid3 ON pensionista_cadastro (instituidor_pensao_uuid);
CREATE INDEX idx_penscad_orig ON pensionista_cadastro (sistema_origem);

CREATE INDEX idx_pensrem_id ON pensionista_remuneracao (id_servidor_portal, ano, mes, sistema_origem);
CREATE INDEX idx_pensrem_uuid ON pensionista_remuneracao (pessoa_uuid);
CREATE INDEX idx_pensrem_orig ON pensionista_remuneracao (sistema_origem);

ALTER TABLE pensionista_cadastro ADD PRIMARY KEY (id_servidor_portal, ano, mes, sistema_origem);
