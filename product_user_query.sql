SELECT prd.brd_name -- 브랜드
	 , ct3_name -- 소분류
     , item_cnt -- 브랜드 소분류별 아이템 갯수
     , F/(F+M) f_pct -- 브랜드 아이템 구매 유저 여성 비율
     , M/(F+M) m_pct -- 브랜드 아이템 구매 유저 남성 비율
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
