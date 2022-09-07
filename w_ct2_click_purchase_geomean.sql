 SELECT  click.w -- 주
	   , click.ct1_name -- 대분류
       , click.ct2_name -- 중분류
       , ct2_c_user_cnt -- 주차별 중분류 클릭 유저 수
       , ct2_p_user_cnt -- 주차별 중분류 구매 유저 수
	   , a.geo_mean geo_mean_a -- 주단위 매출액 상승률 기하 평균
	   , a.ct2_gmv -- 기간 전체 ct2 매출액
	   , b.geo_mean geo_mean_b -- 주단위 클릭 유저 상승률 기하 평균
	   , b.ct2_click_user -- 기간 전체 ct2 클릭 유저
 FROM
		(SELECT 
				 CASE WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-06-03' AND '2021-06-09' THEN '2021-06-03'
				   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-06-10' AND '2021-06-16' THEN '2021-06-10'
				   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-06-17' AND '2021-06-23' THEN '2021-06-17'
				   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-06-24' AND '2021-06-30' THEN '2021-06-24'
				   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-07-01' AND '2021-07-07' THEN '2021-07-01'
				   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-07-08' AND '2021-07-14' THEN '2021-07-08'
				   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-07-15' AND '2021-07-21' THEN '2021-07-15'
				   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-07-22' AND '2021-07-28' THEN '2021-07-22'
				   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-07-29' AND '2021-08-04' THEN '2021-07-29' END w
				 , category1_name ct1_name
				 , category2_name ct2_name
				 , COUNT(DISTINCT user_no) ct2_c_user_cnt
		 FROM event e
			  INNER JOIN product p
			  ON e.item_no = p.item_no
		 WHERE event_name = 'click_item'
		 GROUP BY w, ct1_name, ct2_name) click
		LEFT JOIN 
		(SELECT 
				 CASE WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-06-03' AND '2021-06-09' THEN '2021-06-03'
				   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-06-10' AND '2021-06-16' THEN '2021-06-10'
				   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-06-17' AND '2021-06-23' THEN '2021-06-17'
				   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-06-24' AND '2021-06-30' THEN '2021-06-24'
				   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-07-01' AND '2021-07-07' THEN '2021-07-01'
				   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-07-08' AND '2021-07-14' THEN '2021-07-08'
				   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-07-15' AND '2021-07-21' THEN '2021-07-15'
				   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-07-22' AND '2021-07-28' THEN '2021-07-22'
				   WHEN DATE_FORMAT(event_timestamp, "%Y-%m-%d") BETWEEN '2021-07-29' AND '2021-08-04' THEN '2021-07-29' END w
				 , category1_name ct1_name
				 , category2_name ct2_name
                     , COUNT(DISTINCT user_no) ct2_p_user_cnt
			 FROM event e
				  INNER JOIN product p
                  ON e.item_no = p.item_no
             WHERE event_name = 'purchase_success'
             GROUP BY w, ct1_name, ct2_name) purchase
			ON click.w = purchase.w
                AND click.ct1_name = purchase.ct1_name
                AND click.ct2_name = purchase.ct2_name
		LEFT JOIN A_ct2_geo_mean AS a
			ON click.ct2_name = a.category2_name
		LEFT JOIN B_ct2_geo_mean AS b
			ON click.ct2_name = b.category2_name
	
