select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal
from
	(
		select
			substr(c_phone, 1, 2) as cntrycode,
			c_acctbal
		from
			customer
		where
			substr(c_phone, 1, 2) = '32' or
			substr(c_phone, 1, 2) = '21' or
			substr(c_phone, 1, 2) = '22' or
			substr(c_phone, 1, 2) = '19' or
			substr(c_phone, 1, 2) = '11' or
			substr(c_phone, 1, 2) = '28' or
			substr(c_phone, 1, 2) = '10'

			and c_acctbal > (
				select
					avg(c_acctbal)
				from
					customer
				where
					c_acctbal > 0.00
					and 
						substr(c_phone, 1, 2) = '32' or
						substr(c_phone, 1, 2) = '21' or
						substr(c_phone, 1, 2) = '22' or
						substr(c_phone, 1, 2) = '19' or
						substr(c_phone, 1, 2) = '11' or
						substr(c_phone, 1, 2) = '28' or
						substr(c_phone, 1, 2) = '10'
				)
			and not exists (
				select
					*
				from
					orders
				where
					o_custkey = c_custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode
limit 100;
