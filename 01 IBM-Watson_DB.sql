--SERVER HML: OLESQLHML
--SERVER QA : OLESQLQA
--SERVER PRD: OLESQLREP

/*
rt - contato
início:	06/07/2018	-- dia de jogo do brasil
prazo:	//
fim:	//

1. salvar a saída do script sql "$\mesa_bi\docs\ibm watson\01 atualização master db\01 source\01 master db source.sql" em "$\mesa_bi\docs\ibm watson\01 atualização master db\02 upload\contatos.csv";
2. fazer upload sftp dos arquivos abaixo para o diretório "/upload/" do ibm marketing cloud:
	- "$\mesa_bi\docs\ibm watson\01 atualização master db\02 upload\contatos_map.xml"
	- "$\mesa_bi\docs\ibm watson\01 atualização master db\02 upload\contatos.csv"
3. executar o notebook "$\mesa_bi\docs\jupyter kernels\02 watson\01 atualização db master.ipynb";
4. se execução com sucesso, informar à mesa crm por e-mail.
*/



set nocount on

----------------------------------------------------------------------------------
-- data primeiro contrato cliente empréstimo ( 1.864.340 registros / pessoas )
if object_id('tempdb..#primeiro_contrato_emprestimo') is not null drop table #primeiro_contrato_emprestimo
select
	 emp.cnpj_cpf
	,min(isnull(rep.data_digitacao,emp.data_digitacao))	[data safra]
	--,isnull(rep.data_digitacao,emp.data_digitacao)	[data safra]
into #primeiro_contrato_emprestimo
from
	dm01..emprestimo			emp	with (nolock)
	left join dm01..contrepact	rep	with (nolock) on rep.nro_contrato = emp.nro_contrato
		and rep.data_repactuacao = '1900-01-01'
group by emp.cnpj_cpf

----------------------------------------------------------------------------------
-- data primeiro contrato cliente cartão ( 725.114 registros / 715.786 pessoas ) select count(distinct cnpj_cpf) from #primeiro_contrato_cartao
if object_id('tempdb..#primeiro_contrato_cartao') is not null drop table #primeiro_contrato_cartao
select	-- produção cartão
	 ft1.cnpj_cpf
	,min(isnull(ft1.dtlib,ft1.data_importacao))	[data safra]
into #primeiro_contrato_cartao
from
	bsicar..carcar			car	with (nolock)
	join bsicar..carope		ope	with (nolock) on car.numconcar  = ope.numconcar
	join bsicar..cartipope	tpe	with (nolock) on ope.codtipope = tpe.codtipope
	join sccdem..ft01		ft1	with (nolock) on ft1.numconcar = car.numconcar
	join bsoautoriz..cprop	cp	with (nolock) on ft1.cont = cp.ppnrprop
	join bsoautoriz..cmovp	mp	with (nolock) on mp.mpnrprop = cp.ppnrprop 
	join bsoautoriz..tpara	tp	with (nolock) on tp.pmcodprd = cp.ppcodprdr
where
	1 = 1
	and tp.pmtpoper = '08'
	and mp.mpsit = 'int'
	and tpe.codtipope in (1,3)  -- liberação de crédito via ted/op | liberação de crédito residual	
group by ft1.cnpj_cpf

union

select	-- produção de cartão limite
	 ft1.cnpj_cpf
	,min(ft1.data_importacao)	[data safra]
from
	bsicar..carcar					car	with (nolock)
	left join bsicar..carope		ope	with (nolock) on car.numconcar = ope.numconcar
		and ope.seqope = 1	--pegar a primeira operação que não seja saque inicial (1,3)
	left join bsicar..cartipope		tpe	with (nolock) on ope.codtipope = tpe.codtipope
	inner join sccdem..ft01			ft1	with (nolock) on ft1.numconcar = car.numconcar
	inner join bsoautoriz..cprop	cp	with (nolock) on ft1.cont = cp.ppnrprop
	inner join bsoautoriz..cmovp	mp	with (nolock) on mp.mpnrprop = cp.ppnrprop
	inner join bsoautoriz..tpara	tp	with (nolock) on tp.pmcodprd = cp.ppcodprdr
where
	1 = 1
	and tp.pmtpoper = '08'
	and mp.mpsit = 'int'
	and isnull(tpe.codtipope,-1) not in (1,3,5) -- pegar todas as operações que não sejam saque inicial (1,3) e recompra cartão (5)	
group by ft1.cnpj_cpf


----------------------------------------------------------------------------------
-- datas primeiro contrato cliente empréstimo ou carão ( 2.737.473 registros / 2.294.814 pessoas )
if object_id('tempdb..#datas_primeiro_contrato') is not null drop table #datas_primeiro_contrato
select
	 pce.cnpj_cpf
	,pce.[data safra]
into #datas_primeiro_contrato
from #primeiro_contrato_emprestimo	pce

union

select
	 pcc.cnpj_cpf
	,pcc.[data safra]
from #primeiro_contrato_cartao	pcc



----------------------------------------------------------------------------------
-- menor data contrato ( 2.294.814 registros /  pessoas )
if object_id('tempdb..#primeiro_contrato') is not null drop table #primeiro_contrato
select
	 dpc.cnpj_cpf
	,min(dpc.[data safra])	[data safra]
into #primeiro_contrato
from #datas_primeiro_contrato	dpc
group by dpc.cnpj_cpf

create unique index idx_pc on #primeiro_contrato(cnpj_cpf)

----------------------------------------------------------------------------------
-- datas de cadastro ( 3.738.384 registros / 2.832.606 pessoas )
if object_id('tempdb..#datas_cadastro') is not null drop table #datas_cadastro
select
	 cli.cpf
	,isnull(min(pro.datacadastro),'1900-01-01')	[data cadastro]
into #datas_cadastro
from
	oleoriginacao..proposta		pro	with (nolock)
	join oleoriginacao..cliente	cli	with (nolock) on cli.identificador = pro.identificador
group by cli.cpf

union

select
	 cl.clcpfcgcint
	,isnull(min(cp.ppdtcad),'1900-01-01')
from
	bsoautoriz..cprop		cp	with (nolock)
	join bsoautoriz..cclip	cl	with (nolock) on cl.clcodcli = cp.ppcodcli	
group by cl.clcpfcgcint

union

select
	 cli.cnpj_cpf
	,isnull(min(com.data_compra), '1900-01-01')
from
	dm01..cont_comprados	cc	with (nolock)
	join dm01..compras		com	with (nolock) on com.nro_compra = cc.nro_compra
	join dm01..cliente		cli	with (nolock) on cli.cod_cliente = cc.cod_clie	
group by cli.cnpj_cpf

----------------------------------------------------------------------------------
-- menor data de cadastro ( 2.832.606 registros / pessoas )
if object_id('tempdb..#cadastro') is not null drop table #cadastro
select
	 dc.cpf
	,min(dc.[data cadastro])	[data cadastro]
into #cadastro
from #datas_cadastro	dc
group by dc.cpf

----------------------------------------------------------------------------------
-- base de pessoas do função ( 2.434.598 registros / pessoas )
if object_id('tempdb..#pessoas_funcao') is not null drop table #pessoas_funcao
select
	-- dados da pessoa
	 cl.clcpfcgcint	[cpf]	-- (id)
	,cl.[clnomecli]	[nome]
	,upper(left(left(rtrim(ltrim(cl.[clnomecli])), charindex(' ', rtrim(ltrim(cl.[clnomecli])), 1)), 1))
		+ lower(right(left(rtrim(ltrim(cl.[clnomecli])), charindex(' ', rtrim(ltrim(cl.[clnomecli])), 1)), len(left(rtrim(ltrim(cl.[clnomecli])), charindex(' ', rtrim(ltrim(cl.[clnomecli])), 1)))))	[primeiro nome]
	--,convert(varchar, cp.ppdtcad, 101)	[data cadastro]
	,convert(varchar, c.[data cadastro], 101)	[data cadastro]
	,convert(varchar, pc.[data safra], 101)	[data primeiro contrato]
	,convert(varchar, cl.[cldtnasc], 101)	[data nascimento]
	,cast(round((datediff(day, cl.[cldtnasc], getdate()) / 365.25), 2) as numeric(20,2))	[idade]
	,case cl.[clsexo]
		when 'm' then 'masculino'
		when 'f' then 'feminino'
		else cl.[clsexo]	-- 'não identificado'
	 end		[sexo]
into #pessoas_funcao
from
	bsoautoriz..cprop		cp	with (nolock)
	join bsoautoriz..cclip	cl	with (nolock) on cl.clcodcli = cp.ppcodcli
	join
	(
		select
			 cp2.ppnrprop
			,cp2.ppdtcad
			,row_number() over(partition by cl2.clcpfcgcint order by cp2.ppdtcad desc) row#
		from
			bsoautoriz..cprop		cp2	with (nolock)
			join bsoautoriz..cclip	cl2	with (nolock) on cl2.clcodcli = cp2.ppcodcli
	) ultima on ultima.ppnrprop = cp.ppnrprop
		and ultima.row# = 1
	join #cadastro					c on c.cpf = cl.clcpfcgcint
	left join #primeiro_contrato	pc on pc.cnpj_cpf = cl.clcpfcgcint

create unique index idx_pf on #pessoas_funcao(cpf)

----------------------------------------------------------------------------------
-- base de pessoas do sccd + funcao ( 2.434.598 registros / pessoas )
if object_id('tempdb..#pessoas_sccd') is not null drop table #pessoas_sccd
	select
	 cl.cnpj_cpf	[cpf]
	,nome_cliente collate sql_latin1_general_cp1_ci_as	[nome]
	,upper(left(left(rtrim(ltrim(cl.nome_cliente)), charindex(' ', rtrim(ltrim(cl.nome_cliente)), 1)), 1))
		+ lower(right(left(rtrim(ltrim(cl.nome_cliente)), charindex(' ', rtrim(ltrim(cl.nome_cliente)), 1)), len(left(rtrim(ltrim(cl.nome_cliente)), charindex(' ', rtrim(ltrim(cl.nome_cliente)), 1))))) collate sql_latin1_general_cp1_ci_as	[primeiro nome]

	,convert(varchar, com.data_compra, 101)	[data cadastro]
	,convert(varchar, com.data_compra, 101)	[data primeiro contrato]
		
	,convert(varchar, cl.data_nascimento, 101)	[data nascimento]
	,cast(round((datediff(day, cl.data_nascimento, getdate()) / 365.25), 2) as numeric(20,2))	[idade]

	,case cl.cod_sexo
		when 1 then 'masculino'
		when 2 then 'feminino'
		else convert(varchar, cl.cod_sexo)
	 end	[sexo]
into #pessoas_sccd
from
	dm01..cliente			cl	with (nolock)									
	join dm01..emprestimo	emp	with (nolock) on emp.cod_cliente = cl.cod_cliente	
	join
	(
		select
			 emp2.nro_contrato
			,emp2.data_credito
			,row_number() over(partition by cl2.cnpj_cpf order by emp2.data_credito desc) row#
		from
			dm01..emprestimo	emp2	with (nolock)
			join dm01..cliente	cl2		with (nolock) on cl2.cod_cliente = emp2.cod_cliente
	) ultima on ultima.nro_contrato = emp.nro_contrato
		and ultima.row# = 1
	join dm01..compras com with(nolock)
		on	emp.nro_compra = com.nro_compra
where not exists (select 1 from #pessoas_funcao x where cl.cnpj_cpf = x.[cpf])

union

select * from #pessoas_funcao -- pessoas funcao

create unique index idx_sccd on #pessoas_sccd(cpf)


----------------------------------------------------------------------------------
-- base de pessoas do oleoriginacao + (sccd + funcao) ( 2.434.598 registros / pessoas )
if object_id('tempdb..#pessoas_fim') is not null drop table #pessoas_fim
select
	 convert(numeric,cl.cpf)	[cpf]
	,cl.nome
	,upper(left(left(rtrim(ltrim(cl.nome)), charindex(' ', rtrim(ltrim(cl.nome)), 1)), 1))
		+ lower(right(left(rtrim(ltrim(cl.nome)), charindex(' ', rtrim(ltrim(cl.nome)), 1)), len(left(rtrim(ltrim(cl.nome)), charindex(' ', rtrim(ltrim(cl.nome)), 1)))))	[primeiro nome]
	,convert(varchar, pp.datacadastro, 101)	[data cadastro]
	,convert(varchar, pp.datacadastro, 101)	[data primeiro contrato]	
	,convert(varchar,cl.[datanascimento], 101)	[data nascimento]
	,cast(round((datediff(day, cl.[datanascimento], getdate()) / 365.25), 2) as numeric(20,2))	[idade]	
	,case cl.tiposexo
		when 1 then 'masculino'
		when 2 then 'feminino'
		else convert(varchar,cl.tiposexo)
	 end	[sexo]	
into #pessoas_fim
from
	oleoriginacao..cliente			cl	with (nolock)											-- 34.914 pessoas
	join oleoriginacao..proposta	pp	with (nolock) on pp.identificador = cl.identificador	-- 34.914 pessoas ( join feito apenas para pegar quem fez proposta )
	join
	(
		select
			 pp2.identificador
			,pp2.datacadastro
			,row_number() over(partition by cl2.cpf order by pp2.datacadastro desc) row#
		from
			oleoriginacao..proposta		pp2	with (nolock)
			join oleoriginacao..cliente	cl2	with (nolock) on cl2.identificador = pp2.identificador
	) ultima on ultima.identificador = pp.identificador
		and ultima.row# = 1
where not exists (select 1 from #pessoas_sccd x where 	 convert(numeric,cl.cpf) = x.[cpf])

union

select * from #pessoas_sccd -- funcao + sccd

create unique index idx_fim on #pessoas_fim(cpf)

---------------------------------------------------------------------------------------------
-- validação de óbito ( 839.297 registros )
if object_id('tempdb.dbo.#temp_obito') is not null drop table #temp_obito
select convert(numeric, bsiutil.dbo.retorna_numero(nu_cpf)) as cpf_obito
into #temp_obito
from mailing..obito	with (nolock)

union all

select convert(numeric, bsiutil.dbo.retorna_numero(cnpj_cpf)) as cpf_obito
from dm01..obito	with (nolock)

create index idx_cpf on #temp_obito(cpf_obito)

---------------------------------------------------------------------------------------------
-- validação de listas restritivas (  registros )
if object_id('tempdb.dbo.#bsoautoriz_cnegp')is not null drop table #bsoautoriz_cnegp
select convert(numeric, bsiutil.dbo.retorna_numero(clcgc)) as cpf_cnpj
into #bsoautoriz_cnegp
from bsoautoriz..cnegp	with (nolock)
where
	clcgc is not null
	and clcgc <> ''

create index idx_cpf on #bsoautoriz_cnegp (cpf_cnpj)


---------------------------------------------------------------------------------------------
-- deletando os registros inválidos ( 0 registros )
delete from #pessoas_fim
where
	exists
	(	-- exclusão de clientes que constam na lista restritiva
		select 1
		from #bsoautoriz_cnegp	cn
		where cn.cpf_cnpj = #pessoas_fim.[cpf]
	)	
	or exists
	(	-- exclusão de clientes que constam na tabela de óbito
		select 1
		from #temp_obito	o
		where o.cpf_obito = #pessoas_fim.[cpf]
	)
	or exists
	(	-- exclusão de clientes que constam na tabela de benefiários não participantes de campanha
		select 1
		from mailing..bfcnaoptcpmc	bc	with (nolock)
		where convert(numeric,bc.cpf) = #pessoas_fim.[cpf]
	)
	
----------------------------------------------------------------------------------
-- pegando os telefones no função (  registros / pessoas )
if object_id('tempdb..#telefones_01') is not null drop table #telefones_01
select	-- função 03
	 ccl.cpf					cnpj_cpf 
	,isnull(nullif(ccl.dddcli, ''),'0')	ddd
	,isnull(nullif(ccl.telcli, ''),'0')	telefone
into #telefones_01
from [olegdirep01].[dwhbbs].[dbo].[utb_aux_ctounccli]	ccl	with (nolock)
where
	isnull(nullif(ccl.telcli, ''),'0') <> '0'
	and exists
	(
		select 1
		from #pessoas_fim c
		where c.cpf = ccl.cpf
	)
 

-----------------------------------------------------------------------------
-- removendo caracteres especiais
if object_id('tempdb.dbo.#telefones_02') is not null drop table #telefones_02
select distinct 
	 cnpj_cpf
	,convert(numeric, bsiutil.dbo.retorna_numero(ddd)) as ddd
	,convert(numeric, bsiutil.dbo.retorna_numero(telefone)) as telefone	
into #telefones_02
from #telefones_01
where
	telefone not like '%[a-z]%'
	and ddd not like '%[a-z]%'
	and telefone like '%[0-9]%'
	and ddd like '%[0-9]%'
	and isnumeric(telefone) = 1
	and isnumeric(ddd) = 1
	
create index idx_cnpj_cpf on #telefones_02 (cnpj_cpf)
create index idx_ddd on #telefones_02 (ddd)
create index idx_telefone on #telefones_02 (telefone)
create index idx_2 on #telefones_02 (cnpj_cpf, ddd, telefone)



----------------------------------------------------------
-- cpfs nulos ( 0 registros )
delete from #telefones_02 where isnull(cnpj_cpf,0) = 0

----------------------------------------------------------
-- ddds nulos ( 1.094 registros )
delete from #telefones_02 where isnull(ddd,0) = 0

----------------------------------------------------------
-- telefones nulos ( 582 registros )
delete from #telefones_02 where isnull(telefone,0) = 0

----------------------------------------------------------
-- ddds sem 2 dígitos ( 65 registros )
delete from #telefones_02 where len(ddd) <> 2

----------------------------------------------------------
-- telefones sem 2 dígitos ( 0 registros )
delete from #telefones_02 where len(telefone) not in (8,9)

----------------------------------------------------------
-- telefones base inválida ( 90 registros )
delete t
from #telefones_02 as t
where
	exists
	(
		select 1
		from dwhtra.dbo.bsetel_invalidos	ti	with (nolock)
		where right(cast(t.telefone as varchar),8) = ti.telefone
	)

----------------------------------------------------------
-- incluíndo 9 digito ( 5.773 registros )
--update t 
--set telefone = '9'+ cast(telefone as varchar)	-- select *
--from #telefones_02 as t
--where
--	t.ddd in (select ddd from dwhtra.dbo.bsetel_9_digito) 
--	and len(telefone) = 8 
--	and
--		case 
--			when ddd = 11 and telefone like '[5-9]%' then 1
--			when telefone like '[7-9]%' then 1 
--			else 0
--		end = 1

----------------------------------------------------------
-- telefones com 9 digitos que deveriam ter 8
delete t
from #telefones_02 as t
where
	t.ddd not in
	(
		select ddd
		from dwhtra.dbo.bsetel_9_digito	with (nolock)
	)
	and len(telefone) = 9 

----------------------------------------------------------
-- telefones com 9 digitos com o nono digito menor que 7
delete t
from #telefones_02	t
where
	len(telefone) = 9
	and telefone like '[0-6]%'
 
------------------------------------------------------------
-- base distinta
if object_id('tempdb.dbo.#telefones_03') is not null drop table #telefones_03
select distinct	
	 cnpj_cpf
	,ddd
	,telefone
into #telefones_03
from #telefones_02

create index idx_cnpj_cpf on #telefones_03 (cnpj_cpf)
create index idx_ddd on #telefones_03 (ddd)
create index idx_telefone on #telefones_03 (telefone)

drop table #telefones_02

----------------------------------------------------------
-- telefones repetidos em mais de 2 cpfs
delete t
from #telefones_03 as t
where
	exists
	(
		select 1 
		from
		(
			select
				 telefone
				,ddd
				,count(*) as q
			from #telefones_03 
			group by
				 telefone
				,ddd
			having count(*) > 2
		) as rep
		where
			t.telefone = rep.telefone
			and t.ddd = rep.ddd
	)

----------------------------------------------------------
-- bloqueados no procon sp
delete t
from #telefones_03 as t
where
	exists
	(
		select 1 
		from mailing.dbo.cliente_procon_sp	procon	with (nolock)
		where
			procon.ddd = t.ddd
			and procon.tlf = t.telefone
			and procon.evt = 'bloqueado'
	)

----------------------------------------------------------
-- não querem ser contactados por telefone
--delete t
--from #telefones_03	t
--where
--	exists
--	(
--		select 1 
--		from nbcsql2k8.plusoftcrm.dbo.cs_cdtb_pessoa_pess	pess	with (nolock)
--		where
--			ltrim(rtrim(pess.pess_ds_cgccpf)) <> ''
--			and pess.pess_in_telefone = 's' --telemarketing
--			and convert(numeric,pess.pess_ds_cgccpf) = convert(numeric,t.cnpj_cpf)
--	)

----------------------------------------------------------------------------
-- salvando telefones
if object_id('tempdb..#telefones') is not null drop table #telefones
select 
	 cnpj_cpf
	,isnull(ddd_1, 0) as ddd_1
	,isnull(telefone_1, 0) as telefone_1
	,isnull(ddd_2, 0) as ddd_2
	,isnull(telefone_2, 0) as telefone_2
	,isnull(ddd_3, 0) as ddd_3
	,isnull(telefone_3, 0) as telefone_3
	,isnull(ddd_4, 0) as ddd_4
	,isnull(telefone_4, 0) as telefone_4
	,isnull(ddd_5, 0) as ddd_5
	,isnull(telefone_5, 0) as telefone_5
into #telefones
from (
		select cnpj_cpf, col, value
		from (
			select cnpj_cpf
				,ddd
				,telefone
				,row_number() over(partition by cnpj_cpf order by telefone) as seq
			from #telefones_03
		) b
		cross apply
		(
			select 'ddd_' + cast(seq as varchar) as col, ddd as value union all
			select 'telefone_' + cast(seq as varchar) as col, telefone as value 
		) c (col, value)

	) as t
	pivot
	(
		max(value)
		for col in ([ddd_1],[ddd_2],[ddd_3],[ddd_4],[ddd_5],[ddd_6],
					[telefone_1],[telefone_2],[telefone_3],[telefone_4],[telefone_5],[telefone_6])
 
	) p

create unique index idx on #telefones(cnpj_cpf)

----------------------------------------------------------
-- não querem ser contactados por sms
--delete t
--from #telefones_03 as t
--where
--	exists
--	(
--		select 1 
--		from nbcsql2k8.plusoftcrm.dbo.cs_cdtb_pessoa_pess as pess (nolock)
--		where ltrim(rtrim(pess.pess_ds_cgccpf)) <> '' 
--			and pess.pess_in_sms = 's' --telemarketing
--			and convert(numeric,pess.pess_ds_cgccpf) = convert(numeric,t.cnpj_cpf)
--	)

----------------------------------------------------------------------------
-- salvando telefones celular
if object_id('tempdb..#celular') is not null drop table #celular
select 
	 cnpj_cpf
	,isnull(ddd_1, 0) as ddd_1
	,isnull(telefone_1, 0) as telefone_1
	,isnull(ddd_2, 0) as ddd_2
	,isnull(telefone_2, 0) as telefone_2
	,isnull(ddd_3, 0) as ddd_3
	,isnull(telefone_3, 0) as telefone_3
	,isnull(ddd_4, 0) as ddd_4
	,isnull(telefone_4, 0) as telefone_4
into #celular
from (
		select cnpj_cpf, col, value
		from (
			select cnpj_cpf
				,ddd
				,telefone
				,row_number() over(partition by cnpj_cpf order by telefone) as seq
			from #telefones_03
			where telefone like '[7-9]%'
		) b
		cross apply
		(
			select 'ddd_' + cast(seq as varchar) as col, ddd as value union all
			select 'telefone_' + cast(seq as varchar) as col, telefone as value 
		) c (col, value)

	) as t
	pivot
	(
		max(value)
		for col in ([ddd_1],[ddd_2],[ddd_3],[ddd_4],[ddd_5],[ddd_6],
					[telefone_1],[telefone_2],[telefone_3],[telefone_4],[telefone_5],[telefone_6])
 
	) p

create unique index idx on #celular(cnpj_cpf)

drop table #telefones_03

---------------------------------------------------------------------------------
-- busca de email
if object_id('tempdb.dbo.#email_01') is not null drop table #email_01
select	-- função 01
	 ccl.clcpfcgcint	cnpj_cpf
	,lower(ccl.clemail)	email
	,cldtcad			data_ref
	,'olefun_' + clcodcli collate sql_latin1_general_cp1250_ci_as as id
into #email_01
from bsoautoriz..cclip	ccl	with (nolock)
where
	isnull(ccl.clemail, '') <> ''
	and ccl.clemail not like 'emailpadrao@%.com.br%'
	and ccl.clemail like '%@%'
	and exists
	(
		select 1
		from #pessoas_fim c
		where c.cpf = ccl.clcpfcgcint
	)

create unique index idx on #email_01(id)

create index idx2 on #email_01(cnpj_cpf)
create index idx3 on #email_01(cnpj_cpf,email)
 --(parei aqui)
---------------------------------------------------------------
if object_id('tempdb.dbo.#email_02') is not null drop table #email_02
select distinct 
	 cnpj_cpf
	,e.id
	,e3.seq
	,e.data_ref
	,cast(isnull(replace(replace(replace(ltrim(rtrim(email))
		,'    ',' '),'  ',' '),'  ',' '),'') as varchar(100))  as email
	,case
		when email like '%@%.%'
		then replace(rtrim(substring(email, charindex('@',email), charindex('.', substring(email,  charindex('@',email),  len(email))) - 1 )),'@','') 
		else '' end as dominio_email
	,case
		when email like '%@%'
		then substring(cast(isnull(replace(replace(replace(ltrim(rtrim(email)),'    ',' '),'  ',' '),'  ',' '),'') as varchar(100))
			, charindex('@', cast(isnull(replace(replace(replace(ltrim(rtrim(email)),'    ',' '),'  ',' '),'  ',' '),'') as varchar(100)))
			, len(cast(isnull(replace(replace(replace(ltrim(rtrim(email)),'    ',' '),'  ',' '),'  ',' '),'') as varchar(100)))) 
		else '' end as terminacao_email
	,cast(null as varchar(50)) as status_email
into #email_02
from
	#email_01	e
	inner join
	(
		select
			 id
			,row_number()
				over(partition by cnpj_cpf, left(id, charindex('_', id))
							,cast(isnull(replace(replace(replace(ltrim(rtrim(email))
								,'    ',' '),'  ',' '),'  ',' '),'') as varchar(100))
						order by case when id like 'ole%' then 1 else 2 end 
							,data_ref desc) seqid
			
			,row_number() 
				over(partition by cnpj_cpf
							,cast(isnull(replace(replace(replace(ltrim(rtrim(email))
								,'    ',' '),'  ',' '),'  ',' '),'') as varchar(100))
						order by case when id like 'ole%' then 1 else 2 end 
							,data_ref desc
							,case when id like '%fun%' then 1
								when id like '%dm01%' then 2
								else 9 end) as seqemail
			
			,row_number() 
				over(partition by cnpj_cpf 
						order by case when id like 'ole%' then 1 else 2 end 
							,data_ref desc
							,case when id like '%fun%' then 1
								when id like '%dm01%' then 2
								else 9 end) as seq
		from #email_01 as e2
	) as e3 on e3.id = e.id
		and e3.seqid = 1
		and e3.seqemail = 1

create unique index idx1 on #email_02(id)
create index idx2 on #email_02(cnpj_cpf)
create index idx3 on #email_02(cnpj_cpf,seq)
create index idx4 on #email_02(email, cnpj_cpf)
create index idx5 on #email_02(email)
create index idx6 on #email_02(terminacao_email)
create index idx7 on #email_02(dominio_email)

---------------------------------------------------------------
delete #email_02
where isnull(cnpj_cpf,0) in (0, 99999999999)

---------------------------------------------------------------
-- validacao
-- terminacao do provedor invalido
update #email_02
set status_email = 'invalido'
where
	(email not like '%.br' and email not like '%.com'and email not like '%.net'and email not like '%.org')
	and charindex('@',email) <> 0
	and status_email is null

-- terminacao repetida
update #email_02
set status_email = 'invalido'
where
	(email like '%.com.com%' or email like '%.br.br%' or email like '%.com.br.com.br%' or email like '%.com.gov.br%' or email like '%.gov.com.br%') 
	and status_email is null

-- caractere invalido apos @
update #email_02
set status_email = 'invalido'
where
	(email like '%.br' or email like '%.com'or email like '%.net' or email like '%.org') 
	and status_email is null
	and (replace(rtrim(substring(email, charindex('@',email), len(email))),'@','') not like '[0-9]%' 
	and replace(rtrim(substring(email, charindex('@',email), len(email))),'@','') not like '[a-z]%' )

-- numero apos @
update #email_02
set status_email = 'invalido'
where
	(email like '%.br' or email like '%.com'or email like '%.net' or email like '%.org') 
	and status_email is null
	and (replace(rtrim(substring(email, charindex('@',email), len(email))),'@','') like '[0-9]%')

-- pontos em sequência
update #email_02
set status_email = 'invalido'
where
	(email like '%..%') 
	and status_email is null

-- contem espaço em branco
update #email_02
set status_email = 'invalido'
where
	(ltrim(rtrim(email)) like '% %') 
	and status_email is null

-- contem caractere especial inválido
update #email_02
set status_email = 'invalido'
where (replace(replace(replace(replace(ltrim(rtrim(email)),'.',''),'@',''),'-',''),'_','') like '%[^a-za-z0-9]%') 
and status_email is null

-- endereço de e-mail usado como padrão
update #email_02
set status_email = 'invalido'
where
	email like 'emailpadrao@%.com.br'
	and status_email is null

-- endereço de e-mail repetido em dez clientes ou mais
update #email_02
set status_email = 'invalido'
where
	status_email is null
	and email in
	(
		select email
		from
		(
			select
				 email
				,count(distinct cnpj_cpf) qtd
			from #email_02
			group by email
		) e
		where e.qtd >= 10
	)	

-- provedor invalido
update #email_02
set status_email = 'invalido'
where
	(email like '%.br' or email like '%.com'or email like '%.net' or email like '%.org') 
	and status_email is null
	and dominio_email in
	(
		'sansung'
		,'yahao'
		,'igi'
		,'hormail'
		,'yaooh'
		,'br'
		,'gamail'
		,'gmaill'
		,'uou'
		,'hitmail'
		,'yahho'
		,'samsumg'
		,'igui'
		,'id'
		,'xxx'
		,'yaool'
		,'hotnail'
		,'ual'
		,'hotmasa456957ch524985l'
		,'gemail'
		,'gmmail'
		,'gotmail'
		,'teste'
		,'yah00'
		,'hortmail'
		,'hootmail'
		,'nd'
		,'ouelook'
		,'igcom'
		,'homtial'
		,'fotmail'
		,'hotmaul'
		,'iahoo'
		,'homtmaill'
		,'demaio'
		,'hatomai'
		,'ol'
		,'hotm'
		,'outloo'
		,'yahooo'
		,'gomail'
		,'liver'
		,'gmailo'
		,'outloock'
		,'tahoo'
		,'lavi'
		,'sa456957ch524985ve'
		,'oul'
		,'hotmnail'
		,'boll'
		,'outllook'
		,'vol'
		,'hotmal'
		,'yahhoo'
		,'oulook'
		,'otlook'
		,'igue'
		,'iga'
		,'yarro'
		,'ymial'
		,'amail'
		,'não'
		,'yahoomcom'
		,'uau'
		,'lig'
		,'holmai'
		,'rotmail'
		,'hotmail.gmail'
		,'imail'
		,'yahpp'
		,'gi.com'
	)

-- digitação incorreta
update #email_02
set status_email = 'invalido'
where
	status_email is null
	and terminacao_email in
	(
		 '@yahoocom.br'
		,'@amorhotmail.com'
		,'@bol.cmo.br'
		,'@bol.com.com.br'
		,'@bol.conm.br'
		,'@bool.com.br'
		,'@brwmkt.net'
		,'@com.br'
		,'@coord.far.br'
		,'@dldl.com.br'
		,'@dmgti.co.br'
		,'@e-mail.com'
		,'@email.com'
		,'@fecebook.com'
		,'@gamil.com'
		,'@gamail.com.br'
		,'@gameil.com'
		,'@gamil.com'
		,'@gamil.com.br'
		,'@gameshormail.com'
		,'@gkail.com'
		,'@glbr.com.br'
		,'@globmail.com.br'
		,'@globol.com.br'
		,'@globomail.com'
		,'@gma.com'
		,'@gmaail.com'
		,'@gmai.com'
		,'@gmasa456957ch524985l.com'
		,'@gmailcom.br'
		,'@gmaiol.com'
		,'@gmais.com'
		,'@gmaiu.com'
		,'@gmal.com'
		,'@gmali.com'
		,'@gmeil.com'
		,'@gmial.com'
		,'@gmil.com'
		,'@grazzi.ppg.br'
		,'@h0tmail.com'
		,'@hatimil.com'
		,'@hatmail.com'
		,'@hgotmail.com'
		,'@homail.com'
		,'@homial.com'
		,'@homil.com'
		,'@homtail.com'
		,'@hoptmail.com'
		,'@hot.com'
		,'@hot.mail.com'
		,'@hota.com'
		,'@hotail.com'
		,'@hotamail.com'
		,'@hotamil.com'
		,'@hote-mail.com'
		,'@hotemail.com'
		,'@hotemil.com'
		,'@hotgmail.com'
		,'@hotimail.com'
		,'@hotimal.com'
		,'@hotmaail.com'
		,'@hotmai.com'
		,'@hotmaial.com'
		,'@hotmail.cm.br'
		,'@hotmail.co.br'
		,'@hotmailc.com'
		,'@hotmailcom.br'
		,'@hotmailo.com'
		,'@hotmaio.com'
		,'@hotmaiol.com'
		,'@hotmaiul.com'
		,'@hotmal.com'
		,'@hotmali.com'
		,'@hotmamail.com'
		,'@hotmao.com'
		,'@hotmaoil.com'
		,'@hotmaol.com'
		,'@hotmaqil.com'
		,'@hotmauil.com'
		,'@hotmeil.com'
		,'@hotmel.com'
		,'@hotmel.com.br'
		,'@hotmial.com'
		,'@hotmial.com.br'
		,'@hotmil.com'
		,'@hotmio.com'
		,'@hotmmai.com.br'
		,'@hotmmail.com'
		,'@hotmoil.com'
		,'@hotmsil.com'
		,'@hotrmail.com'
		,'@hott.com.br'
		,'@hottmail.com'
		,'@hotymail.com'
		,'@houtlook.com'
		,'@hoymail.com'
		,'@hptmai.com'
		,'@hptmail.com'
		,'@htimail.com'
		,'@htmail.com'
		,'@htomail.com'
		,'@htotmail.com'
		,'@lcg.cnt.br'
		,'@live.cim'
		,'@live.com.br'
		,'@live.vom.br'
		,'@livemail.com.br'
		,'@nenhum.com.br'
		,'@ohtmail.com'
		,'@oicom.br'
		,'@otmail.com'
		,'@outllok.com'
		,'@outlok.com'
		,'@outloook.com'
		,'@outook.com'
		,'@parecer.srv.br'
		,'@r.com'
		,'@r7.com.br'
		,'@r7.com.com'
		,'@r7l.com'
		,'@rotmail.com'
		,'@rotmail.com.br'
		,'@skyjett.com.com'
		,'@sub.pro.br'
		,'@terr.com.br'
		,'@tim.br'
		,'@tim.com'
		,'@timbrassa456957ch524985l.com.br'
		,'@timbrasil.com.br'
		,'@timmaxitel.com.br'
		,'@tohotmail.com'
		,'@ttim.com.br'
		,'@yaho.com.br'
		,'@yahoo.ccom.br'
		,'@yahoo.cm.br'
		,'@yahoo.com.br'''
		,'@yahoo.con.br'
		,'@yahoocom.br'
		,'@yahopo.com.br'
		,'@yaoo.com'
		,'@yaoo.com.br'
		,'@yaooo.com'
		,'@yarhoo.com.br'
		,'@yaroo.com.br'
		,'@yhaoo.com.br'
		,'@yhoo.com.br'
		,'@yhotmail.com'
		,'@ymai.com'
		,'@yoo.com.br'
		,'@yahooo.com.br'
		,'@ig.om.br'
		,'@yahoo.om.br'
		,'@ig.co9m.br'
		,'@yahoo.vom.br'
		,'@yahoo.co.br'
		,'@bol.coom.br'
		,'@yahoo.coml.br'
		,'@@hotmail.com'
		,'@a7gmail.com'
		,'@ac.gov.com.br'   
		,'@ac.gove.br'  
		,'@aglo.com'  
		,'@ahoo.com.br'
		,'@ai.com.br'
		,'@aim.com'   
		,'@altilok.com.br'        
		,'@altloc.com'
		,'@altlook.com'  
		,'@amail.com'
		,'@aui.com.br'
		,'@autleck.com'           
		,'@autlock.com'
		,'@autlok.com.br'
		,'@autlook.com'
		,'@autlook.com.br'  
		,'@auttook.com'   
		,'@ayahoo.com.br'
		,'@ayahoo.vom.br'   
		,'@b.com.br'
		,'@b0l.com.br'
		,'@banco.com.br'
		,'@bancobomsucesso.com.br'
		,'@bancoboncucesso.com.br'
		,'@bancobonsucesso.com'   
		,'@bancobonsucesso.com.br'
		,'@banrisul.com'       
		,'@baruerei.sp.gov.br'   
		,'@bhotmail.com' 
		,'.co.br'	              
		,'.vom%'	              
		,'.bom.br'               
		,'.cm.br'					
		,'.como.br'				
		,'.con.br'					
		,'.coom.br'				
		,'@gmail.com.br'			
		,'@gmailcom.br'			
		,'@gmail.combr'			
		,'@gmailcom.b'				
		,'@gmailcom.co'			
		,'@gmailcom.c'				
		,'@gmailcom.'				
		,'@gmailcom'				
		,'@gmail.com.'				
		,'@gmail.co'				
		,'@gmail.co.'				
		,'@gmail.c'			  
		,'@gmail.cm'			  
		,'@gmailco'			  
		,'@gmailc'					
		,'@gmail.'					
		,'@gmail'					
		,'@gmail.'					
		,'@gmai'					
		,'@gma'					
		,'@gm'						
		,'@g'						
		,'@gmai.co'				
		,'@gemail.co'				
		,'@gamil.co'				
		,'@hotmail.com.br'			
		,'@hotmailcom.br'			
		,'@hotmail.combr'			
		,'@hot.mail.com.b'			
		,'@hot.mail.com.co'		
		,'@hot.mail.com.c'			
		,'@hotmail.com.'			
		,'@hot.mail.com'			
		,'@hotmailcom'				
		,'@hotmail.co.'			
		,'@hotmal.com.'			
		,'@hotmail.co'				
		,'@hotmail.c'				
		,'@hotmailco'				
		,'@hotmail.cm'				
		,'@hotmailc'				
		,'@hotmaol.'				
		,'@hotmail .'				
		,'@hotm.com.br.'			
		,'@homail.'				
		,'@htomail.'				
		,'@hptmail.'				
		,'@hotmail'				
		,'@hotmai'				  
		,'@hotma'					
		,'@hotm'					
		,'@hot'					
		,'@ho'						
		,'@h'						
		,'@hotmai.co'				
		,'@hotmai.'				
		,'@hotmail.'				
		,'@otmail.co'				
		,'@hotamil.co'				
		,'@homtail.co'				
		,'@hotmil.co'				
		,'@hotmali.co'				
		,'@hotmaol.co'				
		,'@hotmal.co'				
		,'@yahoocombr'				
		,'@yahoo.combr'			
		,'@yahoo.com.b'			
		,'@yahoocom.br'			
		,'@yahoocomb'				
		,'@yahoocom'				
		,'@yahooco'				
		,'@yahoo.co.'				
		,'@yahoo.co'				
		,'@yahoo.c'			  
		,'@yahooc'					
		,'@yahoo.'					
		,'@yahoo'					
		,'@yaho'					
		,'@yah'					
		,'@ya'						
		,'@y'						
		,'@terracombr'				
		,'@terra.combr'			
		,'@terracom.br'			
		,'@terracomb'				
		,'@terracom'				
		,'@terraco'				
		,'@terra.co.'				
		,'@terra.co'				
		,'@terra.c'				
		,'@terrac'					
		,'@terra.'					
		,'@terra'					
		,'@terr'					
		,'@ter'					
		,'@te'						
		,'@t'						
		,'@igcom.br'				
		,'@ig.combr'				
		,'@igcombr'				
		,'@igcomb'					
		,'@igcom'					
		,'@igco'					
		,'@ig.co.'					
		,'@ig.co'					
		,'@ig.c'					
		,'@igi.com.br'			  
		,'@ig.'					
		,'@igc'					
		,'@ig'						
		,'@i'						
		,'@live.com.br'			
		,'@live.combr'				
		,'@livecom.br'				
		,'@live.co'				
		,'@live.co.'				
		,'@live.c'					
		,'@live.'					
		,'@live'					
		,'@liv'			 		
		,'@li'			 			
		,'@l'						
		,'@l'						
		,'@grupobonsucesso.combr'
		,'@grupobonsucesso.comb'
		,'@grupobonsucesso.comr'
		,'@grupobonsucesso.com.'
		,'@grupobonsucesso.com'	
		,'@grupobonsucesso.co.'	
		,'@grupobonsucesso.co'		
		,'@grupobonsucesso.c'		
		,'@grupobonsucesso.'		
		,'@grupobonsucesso'		
		,'@grupobonsucess'			
		,'@grupobonsuces'			
		,'@grupobonsuce'			
		,'@grupobonsuc'			
		,'@grupobonsu'			  
		,'@grupobons'			  
		,'@adv.obsp.org.br'  
		,'@americana.spo.gov.br' 
		,'@bl.com.br' 
		,'@blo.com.br' 
		,'@bo.com.br'   
		,'@bol.br'  
		,'@bol.com' 
		,'@bol.om.br' 
		,'@bolcom.br' 
		,'@bom.com'   
		,'@bom.com.br' 
		,'@bonsuceso.com'
		,'@bonsucesso.com'
		,'@bonsucesso.com.br'
		,'@bonsusseco.com.br1'
		,'@bonsussesso.com'
		,'@bonucesso.com'
		,'@bool.hotmail.com'
		,'@bop.com.br'
		,'@bope.com.br'
		,'@bopl.com.br'
		,'@boul.com.br'
		,'@brtrubo.com.br'
		,'@brturboco.br'
		,'@bunsucesso.com.br'
		,'@cascavel.pr.gpv.br'
		,'@cea.mar.1000.br'
		,'@cascoretora.com'
		,'@cascorretora.com'
		,'@cduc.gov.com.br'
		,'@ceduc.com.gov.br'
		,'@ceduque.am.goolge.com.br'
		,'@cel.g12.br'
		,'@clic21.com.br'
		,'@click21.com'
		,'@click21.com.br'
		,'@clik21.com.br'
		,'@clique21com.br'
		,'@clobo.com'
		,'@contato.com'
		,'@copasa.com'
		,'@correio.com.br'
		,'@correios.com'
		,'@defeasocial.mg.gov.br'
		,'@defecasocial.mg.gov.br'
		,'@defesasoc.mg.gov.br'
		,'@defesasocial.mg.gv.br'
		,'@defesocial.mg.com.br'
		,'@email.com.br'
		,'@embrapa.br'
		,'@embrata.br'
		,'@emtrapa.br'
		,'@epamig.br'
		,'@epmig.br'
		,'@esportes.mg.gove.br'
		,'@example.com'
		,'@examples.com'
		,'@exantle.com'
		,'@expacivasf.com.br'
		,'@expancivasf.com.br'
		,'@expansiva.com.br'
		,'@expansivasf.com.br'
		,'@expansivass.com.br'
		,'@expanssivasf.com.br'
		,'@expasivasf.com.br'
		,'@fabiana.com.br'
		,'@faceboock.com'
		,'@facta.com'
		,'@facta.com.br'
		,'@fazenda.gov.com.br'
		,'@fazenda.rj.gob.br'
		,'@fazenda.rj.gv.br'
		,'@fazendamg.gov.br'
		,'@frupobonsucesso.com.br'
		,'@g-mail.com'
		,'@g.com.br'
		,'@g.mail.com'
		,'@gail.com'
		,'@gemai.com'
		,'@ghotmail.com'
		,'@gi.com.br'
		,'@gimail.com'
		,'@gimal.com'
		,'@gipmail.com.br'
		,'@gitmail.com'
		,'@glaobo.com'
		,'@glob.com'
		,'@globomaeil.com'
		,'@globomai.com'
		,'@globomail.com.br'
		,'@globomeio.com'
		,'@gloemail.com'
		,'@gloo.com'
		,'@gloobo.com'
		,'@gmai.com.br'
		,'@gmaiil.com'
		,'@gmail.842.com'
		,'@gmail1234.com'
		,'@gmailbarbacena.com.br'
		,'@gmailc.com.br'
		,'@gmajl.com'
		,'@gmaqil.com'
		,'@gmasil.com'
		,'@gmauil.com'
		,'@gmaul.com'
		,'@gmeia.com.br'
		,'@gmeil.com.br'
		,'@gmeio.com.br'
		,'@gnail.com'
		,'@gnmail.com'
		,'@gobel.com.br'
		,'@gobo.com'
		,'@gobomail.com'
		,'@golbo.com'
		,'@gonalves@yahoo.com'
		,'@google.com.br'
		,'@gotmai.com'
		,'@gov.br'
		,'@gov.com.br'
		,'@grubobonsucesso.com.br'
		,'@grupo.com.br'
		,'@grupobomsucesso.com'
		,'@grupobomsucesso.com.br'
		,'@grupobonsucess.com.br'
		,'@gupobonsucesso.com'
		,'@gupobonsucesso.com.br'
		,'@gurpobonsucesso.com.br'
		,'@gtmail.com'
		,'@h@oi.com.br'
		,'@haotmail.com'
		,'@hatmail.com.br'
		,'@hayoo.com.br'
		,'@hemoninas.mg.gov.br'
		,'@hgmail.com'
		,'@hhotmail.com'
		,'@himail.com'
		,'@hiotmail.com'
		,'@hmail.com'
		,'@hmotmail.com'
		,'@hmthotmail.com'
		,'@ho0tmail.com'
		,'@ho6tmail.com'
		,'@ho9tmail.com'
		,'@hoamail.com'
		,'@hoatmail.com'
		,'@hoatmeil.com'
		,'@hocketmail.com'
		,'@hohtmail.com'
		,'@hoitmail.com'
		,'@holmail.com'
		,'@holmil.com'
		,'@holtmail.com'
		,'@holtmaiol.com'
		,'@homail.com.br'
		,'@homaitmail.com.br'
		,'@homatil.com'
		,'@homeil.com'
		,'@homtail.com.br'
		,'@homtmail.com'
		,'@hootmaill.com'
		,'@hot.com.br'
		,'@hot.mail.com.br'
		,'@hot5mail.com'
		,'@hotamail.com.br'
		,'@hotamil.com.br'
		,'@hotemail.com.br'
		,'@hotenail.com'
		,'@hotimai.com'
		,'@hotimail.com.br'
		,'@hotimaill.com'
		,'@hotimaiol.com.br'
		,'@hotjmail.com'
		,'@hotlmail.com'
		,'@hotlok.com.br'
		,'@hotlook.com'
		,'@hotlook.com.br'
		,'@hotluk.com'
		,'@hotma.com'
		,'@hotmai.com.br'
		,'@hotmai8l.com'
		,'@hotmai9l.com'
		,'@hotmaii.com'
		,'@hotmaiil.com'
		,'@hotmail.br.com'
		,'@hotmail.coml.com'
		,'@hotmail.commail.com'
		,'@hotmail.gmail.com'
		,'@hotmail.hotmai.com'
		,'@hotmail.mail.com'
		,'@hotmail.om.br'
		,'@hotmail2014.com'
		,'@hotmaila.com'
		,'@hotmailbol.com.br'
		,'@hotmailcom.com'
		,'@hotmailk.com'
		,'@hotmaill.com'
		,'@hotmailmail.com'
		,'@hotmailonline.com'
		,'@hotmails.com'
		,'@hotmaim.com'
		,'@hotmaiol.com.br'
		,'@hotmaisl.com'
		,'@hotmait.com'
		,'@hotmaixl.com'
		,'@hotmamai.com'
		,'@hotmasil.com'
		,'@hotmeil.com.br'
		,'@hotmiail.com'
		,'@hotmil.com.br'
		,'@hotmotmail.com'
		,'@hotmqail.com'
		,'@hotmqil.com'
		,'@hotmsail.com'
		,'@hotmsil.com.br'
		,'@hotmtail.com'
		,'@hotnmail.com'
		,'@hotomail.com'
		,'@hsotmail.com'
		,'@htoamil.com'
		,'@hutmail.com'
		,'@hyotmail.com'
		,'@i10.com.br'
		,'@iayahoo.com.br'
		,'@iayo.com.br'
		,'@iayoo.com.br'
		,'@ibest@ig.com'
		,'@ibst.com'
		,'@ibst.com.br'
		,'@ibsti.com.br'
		,'@iclod.com'
		,'@icloude.com'
		,'@idene.ng.gov.br'
		,'@idraulicacanevaroli.com.br'
		,'@ieadam_hotmail.com'
		,'@ig.br'
		,'@ig.c0m.br'
		,'@ig.ccom.br'
		,'@ig.cim.br'
		,'@ig.copm.br'
		,'@igicom.br'
		,'@iguicom.br'
		,'@iguig.com.br'
		,'@iig.com.br'
		,'@iis.com.br'
		,'@iive.com'
		,'@intermat.com.mt.gov.br'
		,'@intermat.nt.gov.br'
		,'@ipermg.gov.br'
		,'@ipesemg.gov.br'
		,'@ipsemg.br'
		,'@ipseng.mg.gov.br'
		,'@itaborai.rj.gove.br'
		,'@ivest.com.br'
		,'@iveste.com.br'
		,'@ivesti.com.br'
		,'@jesusmail.com.br'
		,'@jhotmail.com'
		,'@jotmail.com'
		,'@justica.nt.gov.br'
		,'@knpç.com'
		,'@l.com'
		,'@l.com.br'
		,'@laive.com'
		,'@lexxionlxxiolexxion.com.br'
		,'@lhotmail.com'
		,'@life.xom.br'
		,'@limão.com.br'
		,'@live1958.com'
		,'@lllflmfdk.com'
		,'@luiz@yahoo.com.br'
		,'@mai.com'
		,'@mail.com'
		,'@mail.org'
		,'@marceloairescom.br'
		,'@mariahotmail.com'
		,'@maringar.prgov.br'
		,'@maringaturismo.com.br'
		,'@matrix1000.com.br'
		,'@mcmclimatizacao.com'
		,'@me.com'
		,'@medijt@hotmail.com'
		,'@miuche@hotmail.com'
		,'@mmimoveis.inb.br'
		,'@montarroioshotmail.com'
		,'@motocahyiamarra.com.br'
		,'@mundialprotora.com.br'
		,'@mwo.br'
		,'@naatem.com.br'
		,'@nao.com'
		,'@nao.com.br'
		,'@naopossi.com'
		,'@naopossui.com'
		,'@naote.com'
		,'@naotem.com'
		,'@naotem.com.br'
		,'@naotem.hotmail.com'
		,'@naotenho.com.br'
		,'@natalrn.gov.br'
		,'@nbol.com.br'
		,'@nhotmail.com'
		,'@nokiaemail.com'
		,'@nos00ruvideos.com.br'
		,'@notmail.com'
		,'@o.com.br'
		,'@oi.br.com'
		,'@oi.om.br'
		,'@oliveriahtmail.com'
		,'@oltlock.com'
		,'@oltlook.com'
		,'@oltlook.com.br'
		,'@ool.com.br'
		,'@ooutlook.com'
		,'@ortilok.com'
		,'@ortolk.com'
		,'@os.com.br'
		,'@otacom.com.br'
		,'@otilook.com'
		,'@otiluk.com'
		,'@otlcook.com'
		,'@otloock.com'
		,'@ottlook.com'
		,'@otulook.com.br'
		,'@ou.com.br'
		,'@oublook.com'
		,'@ouclook.com'
		,'@oultlok.com'
		,'@oultlook.com'
		,'@oultlook.com.br'
		,'@oultook.com'
		,'@ouplook.com'
		,'@oupoiook.com'
		,'@out.com'
		,'@out@outlook.com'
		,'@outbook.com'
		,'@outckool.com'
		,'@outelook.com'
		,'@outilook.com'
		,'@outlcok.com'
		,'@outllok.com.br'
		,'@outlock.com'
		,'@outlock.com.br'
		,'@outlok.com.br'
		,'@outlooc.com'
		,'@outloocka.com'
		,'@outlooik.com'
		,'@outlook.com'
		,'@outlook.com.br'
		,'@outlool.com'
		,'@outloouk.com'
		,'@outluck.com.br'
		,'@outluk.com'
		,'@outluke.com'
		,'@outluook.com'
		,'@outolook.com'
		,'@outook.com.br'
		,'@outoolk.com'
		,'@outtlok.com.br@outtook.com'
		,'@outulook.com'
		,'@outylock.com'
		,'@ouylook.com'
		,'@pc.br.gov.br'
		,'@pc.br.gov.br'
		,'@pdh.gov.br'
		,'@pdh.gov.com'
		,'@panejamento.@rj.gov.br'
		,'@piracibaba.sp.gov.br'
		,'@piracicab.asp.gov.br'
		,'@pjn.juz.br'
		,'@pm.mt.go.br'
		,'@pm.mt.gove.br'
		,'@pm.mt.gr.br'
		,'@pm.mtgov.br'
		,'@pmm.gob.br'
		,'@pmm.gov.com.br'
		,'@pmmg.mg.br'
		,'@pol.com.br'
		,'@policiacivil.br'
		,'@policiacivil.mp.br'
		,'@policiacivil.mt.com.br'
		,'@policiacivil.mt.go.br'
		,'@policiacivil.mt.gor.br'
		,'@policiacivil.nt.gov.br'
		,'@policiamilitar.ft.gov.br'
		,'@policiamilitar.sp.go.br'
		,'@policiamilitarsp.gov.br'
		,'@possui.com.br'
		,'@potencialconsgnado.com.br'
		,'@prof.educacao.gov.com.br'
		,'@prof.educacar.rg.gov.br'
		,'@profeducacao.gov.br'
		,'@professor.bove.br'
		,'@protestobrasilia.lotenoc.br'
		,'@r.7.com'
		,'@r.gov.br'
		,'@receita.gov.cb.br'
		,'@receita.pb.gove.br'
		,'@recipe.pe.gov.br'
		,'@redecredconfiança.com.br'
		,'@requiao.hotmail.com'
		,'@rhyahoo.com.br'
		,'@riocketmail.com'
		,'@rjgmail.com'
		,'@rm.go.br'
		,'@rm.gove.br'
		,'@rm.sed.org.br'
		,'@rn.com.br'
		,'@rn.go.br'
		,'@rn.gov.com.br'
		,'@ro.ov.com.br'
		,'@rocketemail.com'
		,'@rockpmail.com'
		,'@rocktemail.com'
		,'@roketmail.com'
		,'@roqeptmail.com'
		,'@roquetmail.com'
		,'@rothimaio.com'
		,'@rotimeio.com'
		,'@rotket.com.br'
		,'@rotmal.com'
		,'@rr7.com'
		,'@rrhotmail.com'
		,'@rrx.com.br'
		,'@rs.com'
		,'@s.com'
		,'@sad.mtgov.br'
		,'@sad.nt.gov.br'
		,'@samsun.com'
		,'@samung.com'
		,'@sansug.com'
		,'@sansumg.com'
		,'@sansuy.com.br'
		,'@santacatarina-sp.com.br'
		,'@saude.am.gov.com.br'
		,'@saude.ef.gov.br'
		,'@saude.es.br'
		,'@saude.fc.gov.br'
		,'@saude.fc.gov.com.br'
		,'@saude.md.gov.br'
		,'@saude.mg.com.br'
		,'@saude.mggov.br'
		,'@saude.sc.gov.com.br'
		,'@saude.scs.gov.br'
		,'@saude.ses.gov.br'
		,'@saudes.gov.br'
		,'@sav.mt.gov.br'
		,'@sbafff.fbaf.org.com.br'
		,'@sbjuth.mt.gov.br'
		,'@sbpontosenai.br'
		,'@sbpontosenai.br'
		,'@sc.usp.br'
		,'@schaiffler.com'
		,'@seduc.com.gov.br'
		,'@seduc.go.gov.com.br'
		,'@seduc.geo.gov.br'
		,'@seduc.go.gov.com.br'
		,'@seduc.mb.bol.br'
		,'@seduc.mt.go.br'
		,'@sefaez.pi.gov.com.br'
		,'@sefaz.com.gov.br'
		,'@sefaz.mt.gov.com.br'
		,'@sefazaz.pi.gov.br'
		,'@sefin.gov.com.br'
		,'@sejudh.gov.com.br'
		,'@sejudh.mt.gom.br'
		,'@sejudh.mt.mt.gov.br'
		,'@sejuth.mt.gov.com'
		,'@sesaz.mt.go.br'
		,'@sesp.gov.mt.br'
		,'@sesp.mt.gom.br'
		,'@set.rngov.br'
		,'@sks.com'
		,'@soo.sdr.sc.gov.com.br'
		,'@superig.br'
		,'@superigi.com.br'
		,'@superigue.com.br'
		,'@superine.com.br'
		,'@tati_.com.br'
		,'@tera.com.br'
		,'@terrar.com.br'
		,'@tjam.juiz.br'
		,'@tjma.ju.br'
		,'@tjma.juis.br'
		,'@tjma.juz.br'
		,'@tjn.juis.br'
		,'@tjrn.juis.com.br'
		,'@tmail.com'
		,'@tyahho.com.br'
		,'@tyahoo.com.br'
		,'@uaa.com'
		,'@uahoo.com'
		,'@uahoo.com.br'
		,'@uaol.com'
		,'@uaol.com.br'
		,'@ug.com.br'
		,'@ui.com.br'
		,'@ul.com.br'
		,'@ulinho.com'
		,'@uol.comb.br'
		,'@uoll.com.br'
		,'@uoo.com.br'
		,'@uool.com.br'
		,'@uotlook.com'
		,'@uoul.com.br'
		,'@uoutlook.com'
		,'@utelok.com'
		,'@utiluk.com'
		,'@utlook.com'
		,'@whotmail.com'
		,'@wido_.com'
		,'@windonslive.com'
		,'@windowslive.com'
		,'@woutlook.com'
		,'@xxe.sdr.sc.gov.br'
		,'@y-mail.com.br'
		,'@yaahoo.com'
		,'@yaahoo.com.br'
		,'@yafoo.com.br'
		,'@yaghoo.com.br'
		,'@yah.com.br'
		,'@yah0hoo.com.br'
		,'@yah0o.com.br'
		,'@yahaoo.com.br'
		,'@yahhom.com.br'
		,'@yahii.com.br'
		,'@yaho.com'
		,'@yahoi.com.br'
		,'@yahol.com.br'
		,'@yaholl.com'
		,'@yahoo.br'
		,'@yahoo.br.com'
		,'@yahoo.c0m.br'
		,'@yahoo.ciom.br'
		,'@yahoo.cmo.br'
		,'@yahoo.coim.br'
		,'@yahoo.conm.br'
		,'@yahoo.copm.br'
		,'@yahoo.cpm.br'
		,'@yahoo.cvom.br'
		,'@yahoo2012.com.br'
		,'@yahoo33.com.br'
		,'@yahoo9.com.br'
		,'@yahooh.com.br'
		,'@yahoohotmail.com'
		,'@yahool.com'
		,'@yahool.com.br'
		,'@yahoom.com.br'
		,'@yahoou.com.br'
		,'@yahooy.com'
		,'@yahooy.com.br'
		,'@yahpoo.com.br'
		,'@yahu.com'
		,'@yahuu.com.br'
		,'@yahuul.com.br'
		,'@yail.com'
		,'@yamhoo.com.br'
		,'@yanhoo.com.br'
		,'@yanoo.com.br'
		,'@yaoh.com.br'
		,'@yaoho.com.br'
		,'@yaohoo.com'
		,'@yaohoo.com.br' 
		,'@yaon.com.br'
		,'@yaooo.com.br'
		,'@yaqhoo.com.br'
		,'@yaroo.com'
		,'@yarool.com.br'
		,'@yashoo.com.br'
		,'@yau.com.br'
		,'@yawoo.com.br'
		,'@yayhoo.com.br'
		,'@yayoo.br'
		,'@yemail.com'
		,'@yg.com'
		,'@yg.com.br'
		,'@yhahoo.com.br'
		,'@yhaoo.com'
		,'@yhaool.com.br'
		,'@yharoo.com.br'
		,'@yhoo.com'
		,'@yhoo.om.br'
		,'@yhool.com.br'
		,'@ymail.com'
		,'@ymail.com.br'
		,'@ymailo.com'
		,'@ymaio.com'
		,'@ymal.com'
		,'@ymeil.com'
		,'@ymeio.com'
		,'@ynail.com'
		,'@yohaoo.com'
		,'@yohoo.com.br'
		,'@yol.com.br'
		,'@yoll.com.br'
		,'@yoo.com'
		,'@yool.com.br'
		,'@yoolcom.br'
		,'@you.com.br'
		,'@youtube.com'
		,'@ysahoo.com.br'
		,'@yshoo.com.br'
		,'@ytur.com.br'
		,'@yuahho.com.br'
		,'@yv.com'
	)

-- remover emails invalidos
delete #email_02
where status_email is not null

-- exclusão dos clientes que não querem ser contactados por email
if object_id('tempdb.dbo.#nao_contactados')is not null drop table #nao_contactados
select 01583095616 as pess_ds_cgccpf 
into #nao_contactados
--from nbcsql2k8.plusoftcrm.dbo.cs_cdtb_pessoa_pess	pess	with (nolock)
--where
--	ltrim(rtrim(pess.pess_ds_cgccpf)) <> ''
--	and pess.pess_in_email = 's' --email

delete e
from #email_02 e
where
	exists
	(
		select 1
		from #nao_contactados as nc
		where convert(numeric,nc.pess_ds_cgccpf) = e.cnpj_cpf
	)

---------------------------------------------------------------------------------
-- salvando email
if object_id('tempdb..#email') is not null drop table #email
select
	 cnpj_cpf
	,isnull(email_1,'') as email_1
	,isnull(email_2,'') as email_2
	,isnull(email_3,'') as email_3
into #email
from 
	(
		select
			 cnpj_cpf
			,col
			,value
		from
			(
				select
					 cnpj_cpf
					,email
					,row_number() over(partition by cnpj_cpf order by seq) as seq
				from #email_02
			) b
			cross apply
			(
				select 'email_' + cast(seq as varchar) as col, email as value 
			) c (col, value)
	) as t
	pivot
	(
		max(value)
		for col in ([email_1],[email_2],[email_3],[email_4],[email_5])
	) p

create unique index idx on #email(cnpj_cpf)

drop table #email_02 
drop table #nao_contactados;

---------------------------------------------------------------------------------
-- busca de enderecos ( 6.235.956 registros)
if object_id('tempdb.dbo.#enderecos_01') is not null drop table #enderecos_01
select	--função 01
	 ccl.clcpfcgcint	cnpj_cpf
	,upper(clnomecli) collate sql_latin1_general_cp1250_ci_as as nome
	,isnull(cldtalt,cldtcad) as dt
	,upper(clendfis) collate sql_latin1_general_cp1250_ci_as as endereco
	,upper(clnrendfis) collate sql_latin1_general_cp1250_ci_as as numero
	,upper(clcmptfis) collate sql_latin1_general_cp1250_ci_as as complemento
	,upper(clbaifis) collate sql_latin1_general_cp1250_ci_as as bairro
	,upper(clcidfis) collate sql_latin1_general_cp1250_ci_as as cidade
	,upper(cluffis) collate sql_latin1_general_cp1250_ci_as as estado
	,upper(clcepfis) collate sql_latin1_general_cp1250_ci_as as cep
	,'fun_' + clcodcli collate sql_latin1_general_cp1250_ci_as as id
into #enderecos_01
from bsoautoriz..cclip	ccl	with (nolock)
where
	1 = 1
	and exists
	(
		select 1
		from #pessoas_fim c
		where c.cpf = ccl.clcpfcgcint
	)

create unique index idxe on #enderecos_01(id)

---------------------------------------------------------------------------------
-- ultimo endereco alterado/cadastrado
if object_id('tempdb.dbo.#enderecos_02') is not null drop table #enderecos_02
select distinct 
	 cnpj_cpf
	,e.id
	,e3.seq
	,cast(isnull(replace(replace(replace(ltrim(rtrim(nome))
		,'    ',' '),'  ',' '),'  ',' '),'') as varchar(100))  as nome

	,cast(isnull(replace(replace(replace(ltrim(rtrim(endereco))
		,'    ',' '),'    ',' '),'  ',' ')  
		+ ' ' + replace(replace(replace(ltrim(rtrim(numero))
			,'    ',' '),'  ',' '),'  ',' ') 
		+ ' ' + replace(replace(replace(ltrim(rtrim(complemento	))   
			,'    ',' '),'  ',' '),'  ',' '),'') as varchar(100))  as endereco

	,cast(isnull(replace(replace(replace(ltrim(rtrim(bairro))			   
		,'    ',' '),'  ',' '),'  ',' '),'') as varchar(40))  as bairro

	,cast(isnull(replace(replace(replace(ltrim(rtrim(cidade))			   
		,'    ',' '),'  ',' '),'  ',' '),'') as varchar(60))  as cidade

	,cast(isnull(replace(replace(replace(ltrim(rtrim(estado))			   
		,'    ',' '),'  ',' '),'  ',' '),'') as varchar(5))  as estado

	,cast(isnull(replace(replace(replace(ltrim(rtrim(cep))			   
		,'    ',' '),'  ',' '),'  ',' '),'') as varchar(10))  as cep
into #enderecos_02
from #enderecos_01 as e
inner join (
	select id
		,row_number() 
			over(partition by cnpj_cpf 
					order by case 
								when cidade like '%[a-z]%'
								and endereco like '%[a-z1-9]%'
								and cep like '%[1-9]%'
								then 1
								when cidade like '%[a-z]%'
								and cep like '%[1-9]%'
								then 2
								when cep like '%[1-9]%'
								then 3
								else 9 end
							,dt desc) as seq
	from #enderecos_01 as e2
	) as e3
	on e3.id = e.id
	and e3.seq = 1

create unique index idx1 on #enderecos_02(id)
create index idx2 on #enderecos_02(cnpj_cpf)
create index idx3 on #enderecos_02(cnpj_cpf,seq)
create index idx4 on #enderecos_02(nome)
create index idx5 on #enderecos_02(cidade)
create index idx6 on #enderecos_02(bairro)
create index idx7 on #enderecos_02(endereco)

---------------------------------------------------------------------------------
-- ultimo endereco alterado/cadastrado do funcao (reserva)
insert into #enderecos_02
select distinct 
	 cnpj_cpf
	,e.id
	,2 as seq

	,isnull(replace(replace(replace(ltrim(rtrim(nome))
		,'    ',' '),'  ',' '),'  ',' '),'')  as nome

	,isnull(replace(replace(replace(ltrim(rtrim(endereco))
		,'    ',' '),'    ',' '),'  ',' ')  
		+ ' ' + replace(replace(replace(ltrim(rtrim(numero))
			,'    ',' '),'  ',' '),'  ',' ') 
		+ ' ' + replace(replace(replace(ltrim(rtrim(complemento	))   
			,'    ',' '),'  ',' '),'  ',' '),'')  as endereco

	,isnull(replace(replace(replace(ltrim(rtrim(bairro))			   
		,'    ',' '),'  ',' '),'  ',' '),'')  as bairro

	,isnull(replace(replace(replace(ltrim(rtrim(cidade))			   
		,'    ',' '),'  ',' '),'  ',' '),'')  as cidade

	,isnull(replace(replace(replace(ltrim(rtrim(estado))			   
		,'    ',' '),'  ',' '),'  ',' '),'')  as estado

	,isnull(replace(replace(replace(ltrim(rtrim(cep))			   
		,'    ',' '),'  ',' '),'  ',' '),'')  as cep
from #enderecos_01 as e
inner join (
	select id
		,row_number() 
			over(partition by cnpj_cpf 
					order by case 
								when cidade like '%[a-z]%'
								and endereco like '%[a-z1-9]%'
								and cep like '%[1-9]%'
								then 1
								when cidade like '%[a-z]%'
								and cep like '%[1-9]%'
								then 2
								when cep like '%[1-9]%'
								then 3
								else 9 end
							,dt desc) as seq
	from #enderecos_01 as e2
	where e2.id like 'fun%'
		and not exists (select 1 from #enderecos_02 as en
						where en.id = e2.id)
	) as e3
	on e3.id = e.id
	and e3.seq = 1

---------------------------------------------------------------------------------
-- removendo caracteres especiais
update e
set e.nome = e2.nome
from #enderecos_02 e
inner join #enderecos_02 as e2
	on e.cnpj_cpf = e2.cnpj_cpf
	and e.seq = 1
	and e2.seq = 2
where e.nome = ''
	and e2.nome <> ''

update #enderecos_02
set nome = replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(nome
					,char(45),' '),char(46),' '),char(43),' '),char(39),' '),char(47),' ')
					,char(166),' '),char(40),' '),char(180),' '),char(44),' '),char(172),' ')
					,char(95),' '),char(63),' '),char(93),' '),char(96),' '),char(126),' ')
					,char(41),' '),char(91),' '),char(61),' '),char(59),' '),char(176),' ')
					,char(147),' '),char(92),' '),char(129),' '),char(38),' '),char(34),' ')
					,char(146),' '),char(128),' '),char(167),' '),char(94),' '),char(42),' ')
					,char(130),' '),char(137),' ')
where patindex('%[^a-z0-9 ]%', nome) <> 0


update #enderecos_02
set nome = replace(nome,'  ',' ')
where patindex('%  %', nome) <> 0


update #enderecos_02
set cidade = replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(cidade
					,char(45),' '),char(46),' '),char(43),' '),char(39),' '),char(47),' ')
					,char(166),' '),char(40),' '),char(180),' '),char(44),' '),char(172),' ')
					,char(95),' '),char(63),' '),char(93),' '),char(96),' '),char(126),' ')
					,char(41),' '),char(91),' '),char(61),' '),char(59),' '),char(176),' ')
					,char(147),' '),char(92),' '),char(129),' '),char(38),' '),char(34),' ')
					,char(146),' '),char(128),' '),char(167),' '),char(94),' '),char(42),' ')
					,char(130),' '),char(137),' ')
where patindex('%[^a-z0-9 ]%', cidade) <> 0


update #enderecos_02
set cidade = replace(cidade,'  ',' ')
where patindex('%  %', cidade) <> 0


update #enderecos_02
set endereco = replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(endereco
					,char(43),' '),char(166),' '),char(47),' '),char(39),' '),char(40),' ')
					,char(59),' '),char(172),' '),char(58),' '),char(63),' '),char(176),' ')
					,char(92),' '),char(34),' '),char(42),' '),char(180),' '),char(61),' ')
					,char(95),' '),char(96),' '),char(35),' '),char(38),' '),char(93),' ')
					,char(126),' '),char(146),' '),char(91),' '),char(135),' '),char(9),' ')
					,char(160),' '),char(41),' '),char(33),' '),char(129),' '),char(167),' ')
					,char(62),' '),char(128),' '),char(177),' '),char(183),' '),char(182),' ')
					,char(123),' '),char(64),' '),char(137),' '),char(148),' '),char(125),' ')
					,char(173),' '),char(124),' '),char(169),' '),char(37),' '),char(164),' ')
					,char(94),' '),char(134),' '),char(130),' '),char(132),' ')
where patindex('%[^a-z0-9 .,/:]%', endereco) <> 0


update #enderecos_02
	set endereco = ltrim(rtrim(substring(endereco,5, len(endereco))))
where endereco like '[0-9] a %'


update #enderecos_02
	set endereco = ltrim(rtrim(substring(endereco,6, len(endereco))))
where endereco like '[0-9][0-9] a %'


update #enderecos_02
	set endereco = ltrim(rtrim(substring(endereco,4, len(endereco))))
where endereco like '[0-9][^a-z0-9] %'


update #enderecos_02
	set endereco = ltrim(rtrim(substring(endereco,0, patindex('% 000', endereco))))
where endereco like '% 000'

update #enderecos_02
set endereco = replace(endereco,'  ',' ')
where patindex('%  %', endereco) <> 0


update #enderecos_02
set bairro = replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(replace(
				replace(replace(replace(replace(bairro
					,char(43),' '),char(166),' '),char(47),' '),char(39),' '),char(40),' ')
					,char(59),' '),char(172),' '),char(58),' '),char(63),' '),char(176),' ')
					,char(92),' '),char(34),' '),char(42),' '),char(180),' '),char(61),' ')
					,char(95),' '),char(96),' '),char(35),' '),char(38),' '),char(93),' ')
					,char(126),' '),char(146),' '),char(91),' '),char(135),' '),char(9),' ')
					,char(160),' '),char(41),' '),char(33),' '),char(129),' '),char(167),' ')
					,char(62),' '),char(128),' '),char(177),' '),char(183),' '),char(182),' ')
					,char(123),' '),char(64),' '),char(137),' '),char(148),' '),char(125),' ')
					,char(173),' '),char(124),' '),char(169),' '),char(37),' '),char(164),' ')
					,char(94),' '),char(134),' '),char(130),' '),char(132),' ')
where patindex('%[^a-z0-9 .,/:]%', bairro) <> 0


update #enderecos_02
set bairro = replace(bairro,'  ',' ')
where patindex('%  %', bairro) <> 0

drop table #enderecos_01

---------------------------------------------------------------------------------
-- cpfs nulos
delete from #enderecos_02 where isnull(cnpj_cpf,0) = 0
delete from #enderecos_02 where cep = ''

---------------------------------------------------------------------------------
-- ceps e cidades - base correios
if object_id('tempdb..#ceps_sound') is not null drop table #ceps_sound
select 
	 upper(local_log collate sql_latin1_general_cp1250_ci_as )as [cidade]
	,uf_log collate sql_latin1_general_cp1250_ci_as as estado
	,min(cep8_log) collate sql_latin1_general_cp1250_ci_as as [min_cep]
	,max(cep8_log) collate sql_latin1_general_cp1250_ci_as as [max_cep]
	,soundex(replace(local_log collate sql_latin1_general_cp1250_ci_as,' ','')) sound
	,soundex(local_log collate sql_latin1_general_cp1250_ci_as) sound2
	,'sim' as sound_unico
into #ceps_sound 
from cep.sysdba.ceplog2 
group by local_log collate sql_latin1_general_cp1250_ci_as  
	,uf_log  collate sql_latin1_general_cp1250_ci_as 
	,soundex(replace(local_log,' ',''))

update c
	set c.sound_unico = 'não'
from #ceps_sound c 
where exists (select 1
				from #ceps_sound c2
				where c.estado = c2.estado
				and c.sound = c2.sound
				and c.cidade <> c2.cidade)

create unique index idx on #ceps_sound(cidade, estado)
create index idx5 on #ceps_sound(sound_unico,sound, estado)
create index idx6 on #ceps_sound(sound_unico,sound,sound2, estado)
create index idx7 on #ceps_sound([min_cep],[max_cep])


delete from #ceps_sound where min_cep = ''
delete from #ceps_sound where max_cep = ''


---------------------------------------------------------------------------------
-- cidades normalizadas
if object_id('tempdb.dbo.#enderecos_03') is not null drop table #enderecos_03
select 
	cnpj_cpf
	,id	
	,nome	
	,endereco	
	,bairro	
	,isnull(c.cidade,e.cidade) as cidade
	,isnull(c.estado,e.estado) as estado
	,e.cidade as cidade_bse
	,e.estado as estado_bse
	,cep
	,'sim' normalizado
	,'sim' endereco_valido
	,cast(null as varchar(100)) as obs
into #enderecos_03
from #enderecos_02 as e
inner join #ceps_sound as c
	on c.estado = e.estado
	and isnumeric(e.cep) = 1
	--and convert(numeric, e.cep) between c.[min_cep] and c.[max_cep]
	and bsiutil.dbo.retorna_numero(e.cep) between c.[min_cep] and c.[max_cep]
	and c.sound_unico = 'sim' 
	and c.sound = soundex(replace(e.cidade,' ',''))
where e.seq = 1

create index idx on #enderecos_03(cnpj_cpf)
create index idx2 on #enderecos_03(estado)
create index idx3 on #enderecos_03(cidade)
create index idx4 on #enderecos_03(cep)


create index idx_end1 on #enderecos_03(cidade,estado,cep)
create index idx_end2 on #enderecos_03(endereco)
create index idx_end3 on #enderecos_03(bairro)

---------------------------------------------------------------------------------
-- cidades normalizadas sound 2
insert into #enderecos_03
select 
	cnpj_cpf
	,id	
	,nome	
	,endereco	
	,bairro	
	,isnull(c.cidade,e.cidade) as cidade
	,isnull(c.estado,e.estado) as estado
	,e.cidade as cidade_bse
	,e.estado as estado_bse	
	,cep
	,'sim' normalizado
	,'sim' endereco_valido
	,cast(null as varchar(100)) as obs
from #enderecos_02 as e
inner join #ceps_sound as c
	on c.estado = e.estado
	and isnumeric(e.cep) = 1
	and convert(numeric, e.cep) between c.[min_cep] and c.[max_cep]
	and c.sound_unico = 'não' 
	and c.sound = soundex(replace(e.cidade,' ',''))
	and c.sound2 = soundex(e.cidade)
where e.seq = 1
and not exists (select 1 
					from #enderecos_03 as e3
					where e.cnpj_cpf = e3.cnpj_cpf)
		
---------------------------------------------------------------------------------
-- cidades normalizadas distrito federal
insert into #enderecos_03
select 
	cnpj_cpf	
	,id
	,nome	
	,endereco	
	,bairro	
	,e.cidade 
	,e.estado
	,e.cidade as cidade_bse
	,e.estado as estado_bse
	,cep
	,'sim' normalizado
	,'sim' endereco_valido
	,cast(null as varchar(100)) as obs
from #enderecos_02 as e
inner join #ceps_sound as c
	on c.estado = e.estado
	and isnumeric(e.cep) = 1
	and bsiutil.dbo.retorna_numero(e.cep) between c.[min_cep] and c.[max_cep]
where e.seq = 1
	and c.estado = 'df'
	and c.cidade = 'brasilia'
	and e.cidade in (
		 'plano piloto','gama','taguatinga','brazlandia','sobradinho'
		,'planaltina','paranoa','nucleo bandeirante','ceilandia','guara'
		,'cruzeiro','samambaia','santa maria','sao sebastiao','recanto das emas'
		,'lago sul','riacho fundo','lago norte','candangolandia','aguas claras'
		,'riacho fundo ii','sudoeste/octogonal','varjao','park way','scia'
		,'sobradinho ii','jardim botanico','itapoa','sia','vicente pires','fercal'
	)
	and not exists (select 1 
					from #enderecos_03 as e3
					where e.cnpj_cpf = e3.cnpj_cpf)		

---------------------------------------------------------------------------------
-- cidades normalizadas sound 1 >>reserva<<
insert into #enderecos_03
select 
	cnpj_cpf
	,id	
	,nome	
	,endereco	
	,bairro	
	,isnull(c.cidade,e.cidade) as cidade
	,isnull(c.estado,e.estado) as estado
	,e.cidade as cidade_bse
	,e.estado as estado_bse	
	,cep
	,'sim' normalizado
	,'sim' endereco_valido
	,cast(null as varchar(100)) as obs
from #enderecos_02 as e
inner join #ceps_sound as c
	on c.estado = e.estado
	and isnumeric(e.cep) = 1
	and bsiutil.dbo.retorna_numero(e.cep) between c.[min_cep] and c.[max_cep]
	and c.sound_unico = 'sim' 
	and c.sound = soundex(replace(e.cidade,' ',''))
where e.seq = 2
and not exists (select 1 
					from #enderecos_03 as e3
					where e.cnpj_cpf = e3.cnpj_cpf)

---------------------------------------------------------------------------------
-- cidades normalizadas sound 2 >>reserva<<
insert into #enderecos_03
select 
	cnpj_cpf
	,id	
	,nome	
	,endereco	
	,bairro	
	,isnull(c.cidade,e.cidade) as cidade
	,isnull(c.estado,e.estado) as estado	
	,e.cidade as cidade_bse
	,e.estado as estado_bse
	,cep
	,'sim' normalizado
	,'sim' endereco_valido
	,cast(null as varchar(100)) as obs
from #enderecos_02 as e
inner join #ceps_sound as c
	on c.estado = e.estado
	and isnumeric(e.cep) = 1
	and bsiutil.dbo.retorna_numero(e.cep) between c.[min_cep] and c.[max_cep]
	and c.sound_unico = 'não' 
	and c.sound = soundex(replace(e.cidade,' ',''))
	and c.sound2 = soundex(e.cidade)
where e.seq = 2
and not exists (select 1 
					from #enderecos_03 as e3
					where e.cnpj_cpf = e3.cnpj_cpf)
		
---------------------------------------------------------------------------------
-- cidades normalizadas distrito federal >>reserva<<
insert into #enderecos_03
select 
	cnpj_cpf	
	,id
	,nome	
	,endereco	
	,bairro	
	,e.cidade 
	,e.estado as estado	
	,e.cidade as cidade_bse
	,e.estado as estado_bse
	,cep
	,'sim' normalizado
	,'sim' endereco_valido
	,cast(null as varchar(100)) as obs
from #enderecos_02 as e
inner join #ceps_sound as c
	on c.estado = e.estado
	and isnumeric(e.cep) = 1
	and bsiutil.dbo.retorna_numero(e.cep) between c.[min_cep] and c.[max_cep]
where e.seq = 2
	and c.estado = 'df'
	and c.cidade = 'brasilia'
	and e.cidade in (
		 'plano piloto','gama','taguatinga','brazlandia','sobradinho'
		,'planaltina','paranoa','nucleo bandeirante','ceilandia','guara'
		,'cruzeiro','samambaia','santa maria','sao sebastiao','recanto das emas'
		,'lago sul','riacho fundo','lago norte','candangolandia','aguas claras'
		,'riacho fundo ii','sudoeste/octogonal','varjao','park way','scia'
		,'sobradinho ii','jardim botanico','itapoa','sia','vicente pires','fercal'
	)
	and not exists (select 1 
					from #enderecos_03 as e3
					where e.cnpj_cpf = e3.cnpj_cpf)		

---------------------------------------------------------------------------------
-- cidades >>nao<< normalizadas
insert into #enderecos_03
select 
	cnpj_cpf	
	,id
	,nome	
	,endereco	
	,bairro	
	,e.cidade 
	,e.estado
	,e.cidade as cidade_bse
	,e.estado as estado_bse
	,cep
	,'não' normalizado
	,'sim' endereco_valido
	,cast('cidade não normalizada' as varchar(100)) as obs
from #enderecos_02 as e
where e.seq = 1
	and id like 'fun%'
	and not exists (select 1 
					from #enderecos_03 as e3
					where e.cnpj_cpf = e3.cnpj_cpf)		

---------------------------------------------------------------------------------
-- cidades >>nao<< normalizadas >>reserva<<
insert into #enderecos_03
select 
	cnpj_cpf	
	,id
	,nome	
	,endereco	
	,bairro	
	,e.cidade 
	,e.estado
	,e.cidade as cidade_bse
	,e.estado as estado_bse
	,cep
	,'não' normalizado
	,'sim' endereco_valido
	,cast('cidade não normalizada' as varchar(100)) as obs
from #enderecos_02 as e
where e.seq = 2
	and id like 'fun%'
	and not exists (select 1 
					from #enderecos_03 as e3
					where e.cnpj_cpf = e3.cnpj_cpf)		

---------------------------------------------------------------------------------
-- cidades >>nao<< normalizadas >>outros<<
insert into #enderecos_03
select 
	cnpj_cpf	
	,id
	,nome	
	,endereco	
	,bairro	
	,e.cidade 
	,e.estado
	,e.cidade as cidade_bse
	,e.estado as estado_bse
	,cep
	,'não' normalizado
	,'sim' endereco_valido
	,cast('cidade não normalizada' as varchar(100)) as obs
from #enderecos_02 as e
where e.seq = 1
	and not exists (select 1 
					from #enderecos_03 as e3
					where e.cnpj_cpf = e3.cnpj_cpf)		

drop table #enderecos_02
drop table #ceps_sound

---------------------------------------------------------------------------------
-- enderecos repetidos mesmo cpf
if object_id('tempdb.dbo.#enderecos_04') is not null drop table #enderecos_04	
select distinct
	cnpj_cpf
	,id
	,nome
	,endereco
	,bairro
	,cidade_bse as cidade
	,estado_bse as estado
	,cidade_bse
	,estado_bse
	,cep
	,normalizado
	,endereco_valido
	,obs
into #enderecos_04
from #enderecos_03 e
where cnpj_cpf in (select cnpj_cpf 
					from #enderecos_03 
					group by cnpj_cpf 
					having count(1) > 1) 
order by cnpj_cpf

delete e
from #enderecos_03 e
where cnpj_cpf in (select cnpj_cpf 
					from #enderecos_03 
					group by cnpj_cpf 
					having count(1) > 1) 

insert into #enderecos_03
select * from #enderecos_04

drop table #enderecos_04

---------------------------------------------------------------------------------
-- ceps
if object_id('tempdb.dbo.#ceps') is not null drop table #ceps
select distinct upper(ce.local_log) collate sql_latin1_general_cp1250_ci_as	as cidade
		,upper(bairro1_log) collate sql_latin1_general_cp1250_ci_as	as bairro
		,upper(ce.uf_log) collate sql_latin1_general_cp1250_ci_as as estado
		,bsiutil.dbo.retorna_numero(ce.cep8_log) as cep
into #ceps
from cep.sysdba.ceplog2 as ce (nolock) 
where cep8_log like '%[0-9]%'


create index idx1 on #ceps(cep)
create index idx2 on #ceps(cidade)
create index idx3 on #ceps(estado)

---------------------------------------------------------------------------------
-- validar enderecos
update e
	set endereco_valido = 'não'
	,obs = case when obs is not null then obs + ', ' else '' end + 'cep vazio'  
from #enderecos_03 e 
where endereco_valido = 'sim'
and isnumeric(e.cep) = 0

---------------------------------------------------------------------------------
-- validar enderecos
update e
	set endereco_valido = 'não'
	,obs = case when obs is not null then obs + ', ' else '' end + 'cidade vazia'  
from #enderecos_03 e 
where endereco_valido = 'sim'
and cidade = ''

---------------------------------------------------------------------------------
-- validar enderecos
update e
	set endereco_valido = 'não'
	,obs = case when obs is not null then obs + ', ' else '' end + 'estado vazio'  
from #enderecos_03 e 
where endereco_valido = 'sim'
and estado = ''

---------------------------------------------------------------------------------
-- validar enderecos
update e
	set endereco_valido = 'não'
	,obs = case when obs is not null then obs + ', ' else '' end + 'bairro vazio'  
from #enderecos_03 e 
where endereco_valido = 'sim'
and bairro = ''

---------------------------------------------------------------------------------
-- validar enderecos
update e
	set endereco_valido = 'não'
	,obs = case when obs is not null then obs + ', ' else '' end + 'endereco vazio'  
from #enderecos_03 e 
where endereco_valido = 'sim'
and endereco = ''

---------------------------------------------------------------------------------
-- validar enderecos
update e
	set endereco_valido = 'não'
	,obs = case when obs is not null then obs + ', ' else '' end + 'cep inexistente'  
from #enderecos_03 e 
where endereco_valido = 'sim'
and isnumeric(e.cep) = 1
and not exists (select 1
				from #ceps as c 
				where c.cep = convert(numeric, e.cep))

---------------------------------------------------------------------------------
-- validar enderecos
update e
	set endereco_valido = 'não'
	,obs = case when obs is not null then obs + ', ' else '' end + 'cep incompleto'  
from #enderecos_03 e 
where endereco_valido = 'sim'
and isnumeric(e.cep) = 1
and estado <> 'sp'
and len(cep) < 8

---------------------------------------------------------------------------------
-- validar enderecos
update e
	set endereco_valido = 'não'
	,obs = case when obs is not null then obs + ', ' else '' end + 'cep incompleto'  
from #enderecos_03 e 
where endereco_valido = 'sim'
and isnumeric(e.cep) = 1
and estado = 'sp'
and len(cep) < 7

---------------------------------------------------------------------------------
-- validar enderecos
update e
	set endereco_valido = 'não'
	,obs = case when obs is not null then obs + ', ' else '' end + 'cep pertence a outra uf'  
from #enderecos_03 e 
inner join #ceps c 
	on c.cep = convert(numeric, e.cep)
where endereco_valido = 'sim'
and isnumeric(e.cep) = 1
and e.estado <> c.estado

---------------------------------------------------------------------------------
-- validar enderecos
update e
	set endereco_valido = 'não'
	,obs = case when obs is not null then obs + ', ' else '' end + 'logradouro incompleto'  
from #enderecos_03 e 
where endereco_valido = 'sim'
and isnumeric(e.cep) = 1
and endereco not like '%[1-9]%' and
			(		endereco like 'rua %' 
					or endereco like 'r. %' 
					or endereco like 'r_ %'
					or endereco like 'av %' 
			) 
			and  endereco not like '% sn%'
			and endereco not like '% s/n%'
			and endereco not like '% s n%'
			or endereco is null
			or endereco = ''
			or bairro is null
			or bairro = ''
			or cidade is null
			or cidade = ''
			or endereco not like '%[a-z]%'
			
---------------------------------------------------------------------------------
-- enderecos repetidos em mais de 2 clientes
if object_id('tempdb.dbo.#enderecos_05') is not null drop table #enderecos_05	
select 
	endereco
	,bairro
	,cidade
	,estado
	,cep
	,count(cnpj_cpf) qtd_cpfs
into #enderecos_05
from #enderecos_03 e
where endereco_valido = 'sim'
group by 
	endereco
	,bairro
	,cidade
	,estado
	,cep
having count(1) > 2

create index idx1 on #enderecos_05(cidade,estado,cep)
create index idx2 on #enderecos_05(endereco)
create index idx3 on #enderecos_05(bairro)

---------------------------------------------------------------------------------
-- validar enderecos
update e
	set endereco_valido = 'não'
	,obs = case when obs is not null then obs + ', ' else '' end + 'clientes com endereco repetido'  
from #enderecos_03 e 
inner join #enderecos_05 e5 
	on e.endereco = e5.endereco
	and e.bairro = e5.bairro
	and e.cidade = e5.cidade
	and e.estado = e5.estado
	and e.cep = e5.cep
where endereco_valido = 'sim'

drop table #enderecos_05

---------------------------------------------------------------------------------
-- salvando enderecos
if object_id('tempdb..#enderecos') is not null drop table #enderecos
select 
	 cnpj_cpf
	,cast(ltrim(rtrim(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
		 nome,'0',''),'1',''),'2',''),'3',''),'4',''),'5',''),'6',''),'7',''),'8',''),'9',''),'-',''),'=',''))) as varchar(100))	nome
	,cast(endereco as varchar(150))	endereco
	,cast(bairro as varchar(50))	bairro
	,cast(cidade as varchar(50))	cidade
	,cast(estado as varchar(10))	estado
	,cast(cep as varchar(10))		cep
	,cast(endereco_valido as varchar(10)) endereco_valido 
	,isnull(obs,'')					observacao
	,cast(id as varchar(100))		id_endereco
into #enderecos
from #enderecos_03
where cast(endereco_valido as varchar(10)) = 'sim'

create index idx on #enderecos(cnpj_cpf)

----------------------------------------------------------------------------------
-- criando rt - contato - base
if object_id('tempdb..#base') is not null drop table #base
select
	-- dados pessoa
	 pf.[cpf]

	,pf.[nome]
	,pf.[primeiro nome]
	,pf.[data cadastro]
	,pf.[data primeiro contrato]
	,pf.[data nascimento]
	,pf.[idade]
	,pf.[sexo]

	,'(' + convert(varchar, t.ddd_1) + ')' + left(convert(varchar, t.telefone_1), len(convert(varchar, t.telefone_1)) - 4 ) + '-' + right(convert(varchar, t.telefone_1), 4)	[telefone]
	,'55' + convert(varchar, c.ddd_1) + convert(varchar, c.telefone_1)	[celular]
	,''	[whatsapp]
	,e.email_1	[email]
	,end#.cidade
	,end#.estado

	,convert(varchar, getdate(), 101)	[data_ultima_atualizacao]

	,0	[quantidade_simulacao]
	,0	[quantidade_propostas]
	,0	[quantidade_contratos_ativos]
	,0	[quantidade_contratos_inativos]
	,0	[quantidade_contratos_total]
	,''	[perfil_consumo]
	,''	[debito_automatico]

	,''	[fg_propensao_refin]
	,''	[fg_propensao_cartao]
	,''	[fg_propensao_emprestimo]
	,''	[fg_sem_margem]
	,''	[fg_apto_cartao]
	,''	[fg_apto_emprestimo]
	,''	[fg_apto_saque_compl]
	,''	[fg_inadimplente]
	,''	[fg_portabilidade_solicitada]
	,''	[fg_portabilidade_liquidada]
	,''	[fg_portabilidade_retida]
	,''	[fg_contrato_refinanciado_empr]
	,''	[fg_cliente_ativo]
	,''	[fg_cartao_ativo]
	,''	[fg_emprestimo_ativo]
	,''	[fg_saque_compl_ativo]
	,''	[fg_refin_indeferido]
into #base
from
	#pessoas_fim			pf
	left join #telefones	t		on t.cnpj_cpf = pf.cpf
	left join #celular		c		on c.cnpj_cpf = pf.cpf
	left join #email		e		on e.cnpj_cpf = pf.cpf
	left join #enderecos	end#	on end#.cnpj_cpf = pf.cpf

------------------------------------------------------------------------------
-- indicação para bpv ( 979.666 registros )
if object_id('tempdb..#base_bpvs') is not null drop table #base_bpvs
select
	 bbb.[cpf]
	,ind.cod_bpv_1
	,coalesce(ind.bpv_1,'') as bpv_1
	,ind.cod_bpv_2
	,coalesce(ind.bpv_2,'') as bpv_2
	,ind.cod_bpv_3
	,coalesce(ind.bpv_3,'') as bpv_3
	,ind.cod_bpv_4
	,coalesce(ind.bpv_4,'') as bpv_4
into #base_bpvs
from
	(
		select
			 bb.[cpf]
			,bb.cidade
			,bb.estado
			,row_number() over(partition by bb.cidade, bb.estado order by bb.[cpf]) id_cliente_cidade
		from
			(
				select
					 b.[cpf]
					,b.cidade
					,b.estado
				from #base b
				group by
					 b.[cpf]
					,b.cidade
					,b.estado
			) bb --clientes distintos
	) bbb
	inner join dwhtra.dbo.indbpv_cidades	ind	with (nolock) on ind.cidade collate sql_latin1_general_cp1_ci_as = bbb.cidade
		and ind.uf collate sql_latin1_general_cp1_ci_as = bbb.estado
		and (bbb.id_cliente_cidade % ind.qtd_lojas) = ind.ordem

------------------------------------------------------------------------------
-- entrega
if object_id('tempdb..#entrega') is not null drop table #entrega
select
	 b.[cpf]
	,b.[nome]
	,b.[primeiro nome]			[primeiro_nome]
	,b.[data cadastro]			[data_cadastro]
	,b.[data primeiro contrato]	[data_primeiro_contrato]
	,b.[data nascimento]		[data_nascimento]
	,b.[idade]
	,b.[sexo]
	,b.[telefone]
	,b.[celular]
	,b.[whatsapp]
	,b.[email]
	,b.[cidade]
	,b.[estado]

	,bb.bpv_1					[bpv_mais_proxima]
	,upper(left(left(rtrim(ltrim(ag.nome_resp_loginorienta)), charindex(' ', rtrim(ltrim(ag.nome_resp_loginorienta)), 1)), 1))
		+ lower(right(left(rtrim(ltrim(ag.nome_resp_loginorienta)), charindex(' ', rtrim(ltrim(ag.nome_resp_loginorienta)), 1)), len(left(rtrim(ltrim(ag.nome_resp_loginorienta)), charindex(' ', rtrim(ltrim(ag.nome_resp_loginorienta)), 1)))))	[nome_coordenador_bpv]
	,'(0' + left(ltrim(rtrim(replace(ag.tel1,'-',''))), 2) + ') '	-- ddd
		+ left(right(ltrim(rtrim(replace(ag.tel1,'-',''))), len(ltrim(rtrim(replace(ag.tel1,'-','')))) - 2), len(right(ltrim(rtrim(replace(ag.tel1,'-',''))), len(ltrim(rtrim(replace(ag.tel1,'-','')))) - 2)) - 4) + '-'	-- 1ª parte do telefone
		+ right(right(ltrim(rtrim(replace(ag.tel1,'-',''))), len(ltrim(rtrim(replace(ag.tel1,'-','')))) - 2), 4)	[telefone_agente]

	,b.[data_ultima_atualizacao]
	,b.[quantidade_simulacao]
	,b.[quantidade_propostas]
	,b.[quantidade_contratos_ativos]
	,b.[quantidade_contratos_inativos]
	,b.[quantidade_contratos_total]
	,b.[perfil_consumo]
	,b.[debito_automatico]
	,b.[fg_propensao_refin]
	,b.[fg_propensao_cartao]
	,b.[fg_propensao_emprestimo]
	,b.[fg_sem_margem]
	,b.[fg_apto_cartao]
	,b.[fg_apto_emprestimo]
	,b.[fg_apto_saque_compl]
	,b.[fg_inadimplente]
	,b.[fg_portabilidade_solicitada]
	,b.[fg_portabilidade_liquidada]
	,b.[fg_portabilidade_retida]
	,b.[fg_contrato_refinanciado_empr]
	,b.[fg_cliente_ativo]
	,b.[fg_cartao_ativo]
	,b.[fg_emprestimo_ativo]
	,b.[fg_saque_compl_ativo]
	,b.[fg_refin_indeferido]
	,CASE
		WHEN CON.MARGEMDISPONIVEL > 0.0		THEN 'SIM'
		ELSE 'NÃO'
		END														AS MARGEMSUFICIENTE
	,CASE
		WHEN CON.BLOQUEADOEMPRESTIMO = 1 	THEN 'SIM'
		ELSE 'NÃO'
	 END														AS BLOQUEADOEMPRESTIMO
	,CON.MARGEMDISPONIVELCARTAO									AS MARGEMDISPONIVELCARTAO
	,CASE
		WHEN CON.SITUACAOBENEFICIO = 0		THEN 'ATIVO'
		ELSE 'INATIVO'
		END														AS SITUACAOBENEFICIO
	,CONVERT(VARCHAR, CON.DATACONSULTADATAPREV, 101)			AS DATACONSULTADATAPREV
	,ROW_NUMBER() OVER(PARTITION BY b.[cpf] ORDER BY ISNULL(CAST(CON.DATACONSULTADATAPREV AS DATETIME), CAST('19000101' AS DATETIME)) DESC)
																AS ROWNUM
into #entrega
from
	#base						b
	left join #base_bpvs		bb				  on bb.cpf = b.cpf
	left join scac.scac.agente	ag	with (nolock) on ag.cod_agente = bb.cod_bpv_1
	left join OLEBENEFICIARIOINSS.dbo.CONSULTASDADOSBENEFICIOS	AS CON WITH (NOLOCK) ON	CON.CPF = b.[cpf]

------------------------------------------------------------------------------
-- entrega
--/*
IF OBJECT_ID('tempdb.dbo.##WCA_DBMFIM_OLD') IS NOT NULL DROP TABLE ##WCA_DBMFIM_OLD

select
	 isnull([cpf],0)										AS [cpf]
	,LEFT(isnull([nome],''), 255)							AS [nome]
	,LEFT(isnull([primeiro_nome],''), 255)					AS [primeiro_nome]
	,LEFT(isnull([data_cadastro],''), 255)					AS [data_cadastro]
	,LEFT(isnull([data_primeiro_contrato],''), 255)			AS [data_primeiro_contrato]
	,LEFT(isnull([data_nascimento],''), 255)				AS [data_nascimento]
	,LEFT(isnull([idade],0), 255)							AS [idade]
	,LEFT(isnull([sexo],''), 255)							AS [sexo]
	,LEFT(isnull([telefone],''), 255)						AS [telefone]
	,LEFT(isnull([celular],''), 255)						AS [celular]
	,LEFT(isnull([whatsapp],''), 255)						AS [whatsapp]
	,LEFT(isnull([email],''), 255)							AS [email]
	,LEFT(isnull([cidade],''), 255)							AS [cidade]
	,LEFT(isnull([estado],''), 255)							AS [estado]
	,LEFT(isnull([bpv_mais_proxima],''), 255)				AS [bpv_mais_proxima]
	,LEFT(isnull([nome_coordenador_bpv],''), 255)			AS [nome_coordenador_bpv]
	,LEFT(isnull([telefone_agente],''), 255)				AS [telefone_agente]
	,LEFT(isnull([data_ultima_atualizacao],''), 255)		AS [data_ultima_atualizacao]
	,LEFT(isnull([quantidade_simulacao],0), 255)			AS [quantidade_simulacao]
	,LEFT(isnull([quantidade_propostas],0), 255)			AS [quantidade_propostas]
	,LEFT(isnull([quantidade_contratos_ativos],0), 255)		AS [quantidade_contratos_ativos]
	,LEFT(isnull([quantidade_contratos_inativos],0), 255)	AS [quantidade_contratos_inativos]
	,LEFT(isnull([quantidade_contratos_total],0), 255)		AS [quantidade_contratos_total]
	,LEFT(isnull([perfil_consumo],''), 255)					AS [perfil_consumo]
	,LEFT(isnull([debito_automatico],''), 255)				AS [debito_automatico]
	,LEFT(isnull([fg_propensao_refin],''), 255)				AS [fg_propensao_refin]
	,LEFT(isnull([fg_propensao_cartao],''), 255)			AS [fg_propensao_cartao]
	,LEFT(isnull([fg_propensao_emprestimo],''), 255)		AS [fg_propensao_emprestimo]
	,LEFT(isnull([fg_sem_margem],''), 255)					AS [fg_sem_margem]
	,LEFT(isnull([fg_apto_cartao],''), 255)					AS [fg_apto_cartao]
	,LEFT(isnull([fg_apto_emprestimo],''), 255)				AS [fg_apto_emprestimo]
	,LEFT(isnull([fg_apto_saque_compl],''), 255)			AS [fg_apto_saque_compl]
	,LEFT(isnull([fg_inadimplente],''), 255)				AS [fg_inadimplente]
	,LEFT(isnull([fg_portabilidade_solicitada],''), 255)	AS [fg_portabilidade_solicitada]
	,LEFT(isnull([fg_portabilidade_liquidada],''), 255)		AS [fg_portabilidade_liquidada]
	,LEFT(isnull([fg_portabilidade_retida],''), 255)		AS [fg_portabilidade_retida]
	,LEFT(isnull([fg_contrato_refinanciado_empr],''), 255)	AS [fg_contrato_refinanciado_empr]
	,LEFT(isnull([fg_cliente_ativo],''), 255)				AS [fg_cliente_ativo]
	,LEFT(isnull([fg_cartao_ativo],''), 255)				AS [fg_cartao_ativo]
	,LEFT(isnull([fg_emprestimo_ativo],''), 255)			AS [fg_emprestimo_ativo]
	,LEFT(isnull([fg_saque_compl_ativo],''), 255)			AS [fg_saque_compl_ativo]
	,LEFT(isnull([fg_refin_indeferido],''), 255)			AS [fg_refin_indeferido]
	,LEFT(isnull(	CASE
										WHEN MARGEMSUFICIENTE = 'SIM' AND BLOQUEADOEMPRESTIMO = 'NÃO' THEN 'Sim'
										ELSE 'Nao'
									END,''), 255)			AS [fg_elegivel_emprestimo]
	,LEFT(isnull(	CASE
										WHEN MARGEMSUFICIENTE = 'SIM' AND MARGEMDISPONIVELCARTAO > 1 AND SITUACAOBENEFICIO <> 'INATIVO' THEN 'Sim'
										ELSE 'Nao'
									END,''), 255)			AS [fg_elegivel_cartao]
	,LEFT(isnull(	DATACONSULTADATAPREV,''), 255)			AS [data_ultima_consulta_dataprev]
INTO ##WCA_DBMFIM_OLD
from #entrega
where ROWNUM = 1
--*/