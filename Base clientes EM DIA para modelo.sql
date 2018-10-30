SELECT
  PF.CPF AS CPF
  ,P.NOME AS NOME
  ,PF.SEXO
  ,PF.TEMPOEMPREGADO
  ,PF.TEMPOMORADIAANOS
  ,PF.ESTADONATAL
  ,PF.CIDADENATAL
  ,E.DESCRICAO AS ESCOLARIDADE
  ,EC.DESCRICAO AS ESTADOCIVIL
  ,REPLACE(P.DESCRICAO, ',', ' /') AS PROFISSAO
  ,M.DESCRICAO AS MORADIA
  ,O.DESCRICAO AS OCUPACAO
  ,PF.DATAADMISSAOPROFISSAO
  ,COALESCE(CTPF.NUM_DEPENDENTES, 0) NUM_DEPENDENTES
  ,COALESCE(CTPF.NUM_FILHOS, 0) AS NUM_FILHOS
  ,CTPF.POSSUIVEICULO
  ,Sum(LCC.VALOR) AS VLR_ENTRADA
  ,TC.DATACADASTRO
  ,CC.DATAINICIOATRASO
  ,TLC.LIMITEAPLICADO AS "LIMITE"
  ,Trunc(SYSDATE) - CC.DATAINICIOATRASO AS DIAS_ATRASO
FROM
  T_PESSOAFISICA PF
INNER JOIN
  T_CLIENTETITULARPESSOAFISICA CTPF
ON
  PF.IDPESSOAFISICA = CTPF.IDCLIENTETITULARPESSOAFISICA
INNER JOIN
  T_CLIENTEPESSOAFISICA CPF
ON
  CPF.IDCLIENTEPESSOAFISICA = CTPF.IDCLIENTETITULARPESSOAFISICA
LEFT JOIN
  T_CLIENTECOBRANCA CC
ON
  CC.IDCLIENTETITULARPESSOAFISICA = CTPF.IDCLIENTETITULARPESSOAFISICA
INNER JOIN
  T_CADASTRO TC
ON
  TC.IDCADASTRO = PF.IDPESSOAFISICA
INNER JOIN
  T_PESSOA P
ON
  P.IDPESSOA = CTPF.IDCLIENTETITULARPESSOAFISICA
INNER JOIN
  T_LOJA L
ON
  L.IDLOJA = CTPF.IDLOJACADASTRO
INNER JOIN
  T_LIMITECREDITO TLC
ON
  TLC.IDCLIENTETITULARPESSOAFISICA = CTPF.IDCLIENTETITULARPESSOAFISICA
LEFT JOIN
  T_LANCAMENTOCOBRANCACREDIT LCC
ON
  LCC.IDCLIENTECOBRANCA = CC.IDCLIENTECOBRANCA
  AND LCC.IDCLIENTECOBRANCA = CC.IDCLIENTECOBRANCA
INNER JOIN
    T_ESCOLARIDADE E
ON
    PF.IDESCOLARIDADE = E.IDESCOLARIDADE
INNER JOIN
    T_ESTADOCIVIL EC
ON
    PF.IDESTADOCIVIL = EC.IDESTADOCIVIL
INNER JOIN
    T_PROFISSAO P
ON
    PF.IDPROFISSAO = P.IDPROFISSAO
INNER JOIN
    T_MORADIA M
ON
    PF.IDMORADIA = M.IDMORADIA
INNER JOIN
    T_OCUPACAO O
ON
    PF.IDOCUPACAO = O.IDOCUPACAO
WHERE
  --PF.CPF = 35503764803 AND
  CC.IDCLIENTECOBRANCA IS NULL
  AND LCC.IDLANCAMENTOCOBRANCACREDIT IS NULL
  AND TC.DATACADASTRO >= '01/07/18' -- 69.147 EM 30/10
GROUP BY
  PF.CPF
  ,P.NOME
  ,PF.SEXO
  ,PF.TEMPOEMPREGADO
  ,PF.TEMPOMORADIAANOS
  ,PF.ESTADONATAL
  ,PF.CIDADENATAL
  ,E.DESCRICAO
  ,EC.DESCRICAO
  ,P.DESCRICAO
  ,M.DESCRICAO
  ,O.DESCRICAO
  ,PF.DATAADMISSAOPROFISSAO
  ,COALESCE(CTPF.NUM_DEPENDENTES, 0)
  ,COALESCE(CTPF.NUM_FILHOS, 0)
  ,CTPF.POSSUIVEICULO
  ,TC.DATACADASTRO
  ,CC.DATAINICIOATRASO
  ,TLC.LIMITEAPLICADO
  ,Trunc(SYSDATE) - CC.DATAINICIOATRASO
ORDER BY
  TC.DATACADASTRO
;

