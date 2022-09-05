SELECT prd.brd_name
	 , ct3_name
     , item_cnt
     , F/(F+M) f_pct
     , M/(F+M) m_pct
     , age
FROM
		(SELECT brand_name brd_name
			 , category3_name ct3_name
			 , COUNT(DISTINCT item_no) item_cnt
		FROM product
		GROUP BY brand_name, category3_name) prd
	LEFT JOIN
					(
				SELECT brand_name brd_name
                     , SUM(F) F
					 , SUM(M) M
					 , ROUND(AVG(age), 1) age
				FROM product p 
					 INNER JOIN event e
					 ON p.item_no = e.item_no
					 INNER JOIN user u
					 ON e.user_no = u.user_no
				WHERE event_name = 'purchase_success'
				GROUP BY brand_name
				) user
		ON prd.brd_name = user.brd_name