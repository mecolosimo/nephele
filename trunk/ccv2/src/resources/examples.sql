CREATE VIEW mtx_2_viewAS SELECT cvt.nmer, cvt.pi_value FROM comp_vector_t AS cvt JOIN comp_dist_t AS cdt ON (cvt.cd_id = cdt.cd_id)
WHERE cdt.vs_id = 2 AND cvt.window_size BETWEEN 2 AND 3 ;                    
// this works
create temporary table test.tmp (SELECT cvt.nmer, cvt.pi_value FROM comp_vector_t AS cvt JOIN comp_dist_t AS cdt ON (cvt.cd_id = cdt.cd_id)
WHERE cdt.vs_id = 2 AND cvt.window_size BETWEEN 2 AND 3) ;


create view mtx_2_view as SELECT cvt.nmer, cvt.pi_value FROM  comp_vector_t AS cvt JOIN comp_dist_t AS cdt ON (cvt.cd_id = cdt.cd_id) WHERE cdt.vs_id = 2 AND cvt.window_size BETWEEN 2 AND 3;



select count(*) from comp_vector_t AS cvt JOIN comp_dist_t AS cdt ON (cvt.cd_id = cdt.cd_id) WHERE cdt.vs_id = 2 and cvt.window_size = 2;


create view mtx_9t_view as SELECT cvt.nmer, cvt.pi_value FROM  comp_vector_t AS cvt JOIN comp_dist_t AS cdt ON (cvt.cd_id = cdt.cd_id) WHERE cdt.vs_id = 9 AND cvt.window_size BETWEEN 3 AND 9;


-- Deleting sequences
DELETE FROM comp_dist_map_t WHERE cd_id in (SELECT cd_id FROM comp_dist_t WHERE vs_id = **your id**);

DELETE FROM comp_vector_t WHERE cd_id in (SELECT cd_id FROM comp_dist_t WHERE vs_id = **your id**);

DELETE FROM comp_dist_t WHERE vs_id = **your id**;


-- Example
DELETE FROM comp_dist_map_t WHERE cd_id in (SELECT cd_id FROM comp_dist_t WHERE vs_id = 2 AND seq_length < 1000);
DELETE FROM comp_vector_t WHERE cd_id in (SELECT cd_id FROM comp_dist_t WHERE vs_id = 2 AND seq_length < 1000);
DELETE FROM comp_dist_t WHERE vs_id = 2 AND seq_length < 1000;


