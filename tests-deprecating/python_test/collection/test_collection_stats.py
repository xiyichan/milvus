import time
import pdb
import threading
import logging
from multiprocessing import Pool, Process

import pytest
from utils import *
from constants import *

uid = "get_collection_stats"


class TestGetCollectionStats:
    """
    ******************************************************************
      The following cases are used to test `collection_stats` function
    ******************************************************************
    """

    @pytest.fixture(
        scope="function",
        params=gen_invalid_strs()
    )
    def get_invalid_collection_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=gen_simple_index()
    )
    def get_simple_index(self, request, connect):
        # if str(connect._cmd("mode")) == "CPU":
        #     if request.param["index_type"] in index_cpu_not_support():
        #         pytest.skip("CPU not support index_type: ivf_sq8h")
        return request.param

    @pytest.fixture(
        scope="function",
        params=gen_binary_index()
    )
    def get_jaccard_index(self, request, connect):
        logging.getLogger().info(request.param)
        if request.param["index_type"] in binary_support():
            request.param["metric_type"] = "JACCARD"
            return request.param
        else:
            pytest.skip("Skip index Temporary")

    @pytest.fixture(
        scope="function",
        params=[
            1,
            1000,
            2001
        ],
    )
    def insert_count(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_get_collection_stats_name_not_existed(self, connect, collection):
        '''
        target: get collection stats where collection name does not exist
        method: call collection_stats with a random collection_name, which is not in db
        expected: status not ok
        '''
        collection_name = gen_unique_str(uid)
        with pytest.raises(Exception) as e:
            connect.get_collection_stats(collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_get_collection_stats_name_invalid(self, connect, get_invalid_collection_name):
        '''
        target: get collection stats where collection name is invalid
        method: call collection_stats with invalid collection_name
        expected: status not ok
        '''
        collection_name = get_invalid_collection_name
        with pytest.raises(Exception) as e:
            connect.get_collection_stats(collection_name)

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_get_collection_stats_empty(self, connect, collection):
        '''
        target: get collection stats where no entity in collection
        method: call collection_stats in empty collection
        expected: segment = []
        '''
        stats = connect.get_collection_stats(collection)
        connect.flush([collection])
        assert stats[row_count] == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_get_collection_stats_without_connection(self, collection, dis_connect):
        '''
        target: test count_entities, without connection
        method: calling count_entities with correct params, with a disconnected instance
        expected: count_entities raise exception
        '''
        with pytest.raises(Exception) as e:
            dis_connect.get_collection_stats(collection)

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_get_collection_stats_batch(self, connect, collection):
        '''
        target: get row count with collection_stats
        method: add entities, check count in collection info
        expected: count as expected
        '''
        ids = connect.insert(collection, default_entities)
        assert len(ids) == default_nb
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert int(stats[row_count]) == default_nb

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_get_collection_stats_single(self, connect, collection):
        '''
        target: get row count with collection_stats
        method: add entity one by one, check count in collection info
        expected: count as expected
        '''
        nb = 10
        for i in range(nb):
            connect.insert(collection, default_entity)
            connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == nb

    @pytest.mark.tags(CaseLabel.L2)
    def _test_get_collection_stats_after_delete(self, connect, collection):
        '''
        target: get row count with collection_stats
        method: add and delete entities, check count in collection info
        expected: status ok, count as expected
        '''
        ids = connect.insert(collection, default_entities)
        status = connect.flush([collection])
        delete_ids = [ids[0], ids[-1]]
        connect.delete_entity_by_id(collection, delete_ids)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats["row_count"] == default_nb - 2
        assert stats["partitions"][0]["row_count"] == default_nb - 2
        assert stats["partitions"][0]["segments"][0]["data_size"] > 0

    # TODO: enable
    @pytest.mark.tags(CaseLabel.L2)
    def _test_get_collection_stats_after_compact_parts(self, connect, collection):
        '''
        target: get row count with collection_stats
        method: add and delete entities, and compact collection, check count in collection info
        expected: status ok, count as expected
        '''
        delete_length = 1000
        ids = connect.insert(collection, default_entities)
        status = connect.flush([collection])
        delete_ids = ids[:delete_length]
        connect.delete_entity_by_id(collection, delete_ids)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        logging.getLogger().info(stats)
        assert stats["row_count"] == default_nb - delete_length
        compact_before = stats["partitions"][0]["segments"][0]["data_size"]
        connect.compact(collection)
        stats = connect.get_collection_stats(collection)
        logging.getLogger().info(stats)
        compact_after = stats["partitions"][0]["segments"][0]["data_size"]
        assert compact_before == compact_after

    @pytest.mark.tags(CaseLabel.L2)
    def _test_get_collection_stats_after_compact_delete_one(self, connect, collection):
        '''
        target: get row count with collection_stats
        method: add and delete one entity, and compact collection, check count in collection info
        expected: status ok, count as expected
        '''
        ids = connect.insert(collection, default_entities)
        status = connect.flush([collection])
        delete_ids = ids[:1]
        connect.delete_entity_by_id(collection, delete_ids)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        logging.getLogger().info(stats)
        compact_before = stats["partitions"][0]["row_count"]
        connect.compact(collection)
        stats = connect.get_collection_stats(collection)
        logging.getLogger().info(stats)
        compact_after = stats["partitions"][0]["row_count"]
        # pdb.set_trace()
        assert compact_before == compact_after

    @pytest.mark.tags(CaseLabel.L2)
    def test_get_collection_stats_partition(self, connect, collection):
        '''
        target: get partition info in a collection
        method: call collection_stats after partition created and check partition_stats
        expected: status ok, vectors added to partition
        '''
        connect.create_partition(collection, default_tag)
        ids = connect.insert(collection, default_entities, partition_name=default_tag)
        assert len(ids) == default_nb
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == default_nb

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_get_collection_stats_partitions(self, connect, collection):
        '''
        target: get partition info in a collection
        method: create two partitions, add vectors in one of the partitions, call collection_stats and check
        expected: status ok, vectors added to one partition but not the other
        '''
        new_tag = "new_tag"
        connect.create_partition(collection, default_tag)
        connect.create_partition(collection, new_tag)
        connect.insert(collection, default_entities, partition_name=default_tag)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == default_nb
        connect.insert(collection, default_entities, partition_name=new_tag)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == default_nb * 2
        connect.insert(collection, default_entities)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == default_nb * 3

    @pytest.mark.tags(CaseLabel.L2)
    def test_get_collection_stats_partitions_A(self, connect, collection, insert_count):
        '''
        target: test collection rows_count is correct or not
        method: create collection, create partitions and add entities in it,
            assert the value returned by count_entities method is equal to length of entities
        expected: the count is equal to the length of entities
        '''
        new_tag = "new_tag"
        entities = gen_entities(insert_count)
        connect.create_partition(collection, default_tag)
        connect.create_partition(collection, new_tag)
        connect.insert(collection, entities)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == insert_count

    @pytest.mark.tags(CaseLabel.L2)
    def test_get_collection_stats_partitions_B(self, connect, collection, insert_count):
        '''
        target: test collection rows_count is correct or not
        method: create collection, create partitions and add entities in one of the partitions,
            assert the value returned by count_entities method is equal to length of entities
        expected: the count is equal to the length of entities
        '''
        new_tag = "new_tag"
        entities = gen_entities(insert_count)
        connect.create_partition(collection, default_tag)
        connect.create_partition(collection, new_tag)
        connect.insert(collection, entities, partition_name=default_tag)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == insert_count

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_get_collection_stats_partitions_C(self, connect, collection, insert_count):
        '''
        target: test collection rows_count is correct or not
        method: create collection, create partitions and add entities in one of the partitions,
            assert the value returned by count_entities method is equal to length of entities
        expected: the count is equal to the length of vectors
        '''
        new_tag = "new_tag"
        entities = gen_entities(insert_count)
        connect.create_partition(collection, default_tag)
        connect.create_partition(collection, new_tag)
        connect.insert(collection, entities)
        connect.insert(collection, entities, partition_name=default_tag)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == insert_count*2

    @pytest.mark.tags(CaseLabel.L2)
    def test_get_collection_stats_partitions_D(self, connect, collection, insert_count):
        '''
        target: test collection rows_count is correct or not
        method: create collection, create partitions and add entities in one of the partitions,
            assert the value returned by count_entities method is equal to length of entities
        expected: the collection count is equal to the length of entities
        '''
        new_tag = "new_tag"
        entities = gen_entities(insert_count)
        connect.create_partition(collection, default_tag)
        connect.create_partition(collection, new_tag)
        connect.insert(collection, entities, partition_name=default_tag)
        connect.insert(collection, entities, partition_name=new_tag)
        connect.flush([collection])
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == insert_count*2

    # TODO: assert metric type in stats response
    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_get_collection_stats_after_index_created(self, connect, collection, get_simple_index):
        '''
        target: test collection info after index created
        method: create collection, add vectors, create index and call collection_stats
        expected: status ok, index created and shown in segments
        '''
        connect.insert(collection, default_entities)
        connect.flush([collection])
        connect.create_index(collection, default_float_vec_field_name, get_simple_index)
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == default_nb

    # TODO: assert metric type in stats response
    @pytest.mark.tags(CaseLabel.L2)
    def test_get_collection_stats_after_index_created_ip(self, connect, collection, get_simple_index):
        '''
        target: test collection info after index created
        method: create collection, add vectors, create index and call collection_stats
        expected: status ok, index created and shown in segments
        '''
        get_simple_index["metric_type"] = "IP"
        ids = connect.insert(collection, default_entities)
        assert len(ids) == default_nb
        connect.flush([collection])
        get_simple_index.update({"metric_type": "IP"})
        connect.create_index(collection, default_float_vec_field_name, get_simple_index)
        stats = connect.get_collection_stats(collection)
        assert stats[row_count] == default_nb

    # TODO: assert metric type in stats response
    @pytest.mark.tags(CaseLabel.L2)
    def test_get_collection_stats_after_index_created_jac(self, connect, binary_collection, get_jaccard_index):
        '''
        target: test collection info after index created
        method: create collection, add binary entities, create index and call collection_stats
        expected: status ok, index created and shown in segments
        '''
        ids = connect.insert(binary_collection, default_binary_entities)
        connect.flush([binary_collection])
        connect.create_index(binary_collection, default_binary_vec_field_name, get_jaccard_index)
        stats = connect.get_collection_stats(binary_collection)
        assert stats[row_count] == default_nb

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_get_collection_stats_after_create_different_index(self, connect, collection):
        '''
        target: test collection info after index created repeatedly
        method: create collection, add vectors, create index and call collection_stats multiple times
        expected: status ok, index info shown in segments
        '''
        ids = connect.insert(collection, default_entities)
        connect.flush([collection])
        for index_type in ["IVF_FLAT", "IVF_SQ8"]:
            connect.create_index(collection, default_float_vec_field_name,
                                 {"index_type": index_type, "params": {"nlist": 1024}, "metric_type": "L2"})
            stats = connect.get_collection_stats(collection)
            assert stats[row_count] == default_nb

    @pytest.mark.tags(CaseLabel.tags_smoke)
    def test_collection_count_multi_collections(self, connect):
        '''
        target: test collection rows_count is correct or not with multiple collections of L2
        method: create collection and add entities in it,
            assert the value returned by count_entities method is equal to length of entities
        expected: row count in segments
        '''
        collection_list = []
        collection_num = 10
        for i in range(collection_num):
            collection_name = gen_unique_str(uid)
            collection_list.append(collection_name)
            connect.create_collection(collection_name, default_fields)
            ids = connect.insert(collection_name, default_entities)
        connect.flush(collection_list)
        for i in range(collection_num):
            stats = connect.get_collection_stats(collection_list[i])
            assert stats[row_count] == default_nb
            connect.drop_collection(collection_list[i])

    @pytest.mark.tags(CaseLabel.L2)
    def test_collection_count_multi_collections_indexed(self, connect):
        '''
        target: test collection rows_count is correct or not with multiple collections of L2
        method: create collection and add entities in it,
            assert the value returned by count_entities method is equal to length of entities
        expected: row count in segments
        '''
        collection_list = []
        collection_num = 10
        for i in range(collection_num):
            collection_name = gen_unique_str(uid)
            collection_list.append(collection_name)
            connect.create_collection(collection_name, default_fields)
            res = connect.insert(collection_name, default_entities)
            connect.flush(collection_list)
            index_1 = {"index_type": "IVF_SQ8", "params": {"nlist": 1024}, "metric_type": "L2"}
            index_2 = {"index_type": "IVF_FLAT", "params": {"nlist": 1024}, "metric_type": "L2"}
            if i % 2:
                connect.create_index(collection_name, default_float_vec_field_name, index_1)
            else:
                connect.create_index(collection_name, default_float_vec_field_name, index_2)
        for i in range(collection_num):
            stats = connect.get_collection_stats(collection_list[i])
            assert stats[row_count] == default_nb
            index = connect.describe_index(collection_list[i], "")
            if i % 2:
                create_target_index(index_1, default_float_vec_field_name)
                assert index == index_1
            else:
                create_target_index(index_2, default_float_vec_field_name)
                assert index == index_2
                # break
            connect.drop_collection(collection_list[i])
