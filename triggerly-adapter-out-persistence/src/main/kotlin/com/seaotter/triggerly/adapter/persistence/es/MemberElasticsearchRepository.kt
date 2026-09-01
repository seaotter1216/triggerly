package com.seaotter.triggerly.adapter.persistence.es

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository

interface MemberElasticsearchRepository : ElasticsearchRepository<MemberDocument, String>
