SELECT
       L.NOMEFANTASIA AS NOMEFANTASIA,
       PF.CPF AS CPF,
       P.NOME AS NOME,
       Sum(LCC.VALOR) AS VLR_ENTRADA,
       TC.DATACADASTRO
       ,CC.DATAINICIOATRASO,
       TLC.LIMITEAPLICADO AS "LIMITE"
       ,Trunc(SYSDATE) - CC.DATAINICIOATRASO AS DIAS_ATRASO
FROM   T_PESSOAFISICA PF,
       T_CLIENTETITULARPESSOAFISICA CTPF,
       T_CLIENTEPESSOAFISICA CPF,
       T_CLIENTECOBRANCA CC,
       T_CADASTRO TC,
       T_PESSOA P,
       T_LOJA L,
       T_LIMITECREDITO TLC
       ,T_LANCAMENTOCOBRANCACREDIT LCC
WHERE  PF.IDPESSOAFISICA = CTPF.IDCLIENTETITULARPESSOAFISICA
AND    CPF.IDCLIENTEPESSOAFISICA = CTPF.IDCLIENTETITULARPESSOAFISICA
AND    CC.IDCLIENTETITULARPESSOAFISICA = CTPF.IDCLIENTETITULARPESSOAFISICA
AND    TC.IDCADASTRO = PF.IDPESSOAFISICA
AND    CC.STATUS <> 'I'
AND    L.IDLOJA = CTPF.IDLOJACADASTRO
AND    P.IDPESSOA = CTPF.IDCLIENTETITULARPESSOAFISICA
AND    LCC.IDCLIENTECOBRANCA = CC.IDCLIENTECOBRANCA
AND    TLC.IDCLIENTETITULARPESSOAFISICA = CTPF.IDCLIENTETITULARPESSOAFISICA
AND    TC.DATACADASTRO >= '01/01/18'
AND    NOT EXISTS (SELECT 'X' FROM T_PAGAMENTO P WHERE P.IDCLIENTETITULARPESSOAFISICA = CTPF.IDCLIENTETITULARPESSOAFISICA AND STATUS = 'N' AND MEIOTRANSACAO <> 'C')
AND    NOT EXISTS (SELECT 'X' FROM T_PAGAMENTOBOLETO P WHERE P.IDCLIENTETITULARPESSOAFISICA = CTPF.IDCLIENTETITULARPESSOAFISICA AND STATUS = 'N')
GROUP BY CTPF.IDCLIENTETITULARPESSOAFISICA, CTPF.IDLOJACADASTRO, L.NOMEFANTASIA, PF.CPF, P.NOME, CC.DATAINICIOATRASO,TC.DATACADASTRO,TLC.LIMITEAPLICADO
ORDER BY TC.DATACADASTRO
;