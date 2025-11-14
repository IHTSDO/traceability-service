package org.ihtsdo.otf.traceabilityservice.util;

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.json.JsonData;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Wrapping Elastic's builders.
 */
public class QueryHelper {

    /**
     * Storage for RangeQuery field and values, keyed by builder instance
     */
    private static class RangeQueryState {
        String field;
        JsonData fromValue;
        JsonData toValue;
    }

    private static final Map<RangeQuery.Builder, RangeQueryState> rangeQueryStates = new ConcurrentHashMap<>();
    /*
     * BoolQuery
     * */
    public static Query toQuery(BoolQuery.Builder builder) {
        return builder.build()._toQuery();
    }

    /*
     * TermQuery
     * */
    public static TermQuery.Builder termQueryBuilder() {
        return new TermQuery.Builder();
    }

    public static void withFieldValue(TermQuery.Builder builder, String field, String value) {
        builder.field(field).value(value);
    }

    public static void withFieldValue(TermQuery.Builder builder, String field, long value) {
        builder.field(field).value(value);
    }

    public static Query toQuery(TermQuery.Builder builder) {
        return builder.build()._toQuery();
    }

    public static Query termQuery(String field, String value) {
        TermQuery.Builder builder = termQueryBuilder();
        withFieldValue(builder, field, value);
        return toQuery(builder);
    }

    public static Query termQuery(String field, long value) {
        TermQuery.Builder builder = termQueryBuilder();
        withFieldValue(builder, field, value);
        return toQuery(builder);
    }

    /*
     * TermsQuery
     * */
    public static TermsQuery.Builder termsQueryBuilder() {
        return new TermsQuery.Builder();
    }

    public static void withFieldValue(TermsQuery.Builder builder, String field, Collection<?> values) {
        builder.field(field).terms(tq -> tq.value(values.stream().map(JsonData::of).map(FieldValue::of).collect(Collectors.toList())));
    }

    public static Query toQuery(TermsQuery.Builder builder) {
        return builder.build()._toQuery();
    }

    public static Query termsQuery(String field, Collection<?> values) {
        TermsQuery.Builder builder = termsQueryBuilder();
        withFieldValue(builder, field, values);
        return toQuery(builder);
    }

    /*
     * PrefixQuery
     * */
    public static PrefixQuery.Builder prefixQueryBuilder() {
        return new PrefixQuery.Builder();
    }

    public static void withFieldValue(PrefixQuery.Builder builder, String field, String value) {
        builder.field(field).value(value);
    }

    public static Query toQuery(PrefixQuery.Builder builder) {
        return builder.build()._toQuery();
    }

    public static Query prefixQuery(String field, String value) {
        PrefixQuery.Builder builder = prefixQueryBuilder();
        withFieldValue(builder, field, value);
        return toQuery(builder);
    }

    /*
     * RangeQuery
     * */
    public static RangeQuery.Builder rangeQueryBuilder() {
        RangeQuery.Builder builder = new RangeQuery.Builder();
        rangeQueryStates.put(builder, new RangeQueryState());
        return builder;
    }

    public static RangeQuery.Builder rangeQueryBuilder(String field) {
        RangeQuery.Builder builder = new RangeQuery.Builder();
        RangeQueryState state = new RangeQueryState();
        state.field = field;
        rangeQueryStates.put(builder, state);
        return builder;
    }

    public static void withField(RangeQuery.Builder rangeQueryBuilder, String field) {
        RangeQueryState state = rangeQueryStates.computeIfAbsent(rangeQueryBuilder, k -> new RangeQueryState());
        state.field = field;
    }

    public static void withFrom(RangeQuery.Builder rangeQueryBuilder, String value) {
        RangeQueryState state = rangeQueryStates.computeIfAbsent(rangeQueryBuilder, k -> new RangeQueryState());
        state.fromValue = JsonData.of(value);
    }

    public static void withFrom(RangeQuery.Builder rangeQueryBuilder, long value) {
        RangeQueryState state = rangeQueryStates.computeIfAbsent(rangeQueryBuilder, k -> new RangeQueryState());
        state.fromValue = JsonData.of(value);
    }

    public static void withTo(RangeQuery.Builder rangeQueryBuilder, String value) {
        RangeQueryState state = rangeQueryStates.computeIfAbsent(rangeQueryBuilder, k -> new RangeQueryState());
        state.toValue = JsonData.of(value);
    }

    public static void withTo(RangeQuery.Builder rangeQueryBuilder, long value) {
        RangeQueryState state = rangeQueryStates.computeIfAbsent(rangeQueryBuilder, k -> new RangeQueryState());
        state.toValue = JsonData.of(value);
    }

    public static Query toQuery(RangeQuery.Builder rangeQueryBuilder) {
        RangeQueryState state = rangeQueryStates.remove(rangeQueryBuilder);
        if (state != null && state.field != null) {
            co.elastic.clients.elasticsearch._types.query_dsl.UntypedRangeQuery.Builder untypedBuilder = 
                new co.elastic.clients.elasticsearch._types.query_dsl.UntypedRangeQuery.Builder()
                    .field(state.field);
            if (state.fromValue != null) {
                untypedBuilder.gte(state.fromValue);
            }
            if (state.toValue != null) {
                untypedBuilder.lte(state.toValue);
            }
            RangeQuery.Builder builder = new RangeQuery.Builder();
            builder.untyped(untypedBuilder.build());
            return builder.build()._toQuery();
        }
        return rangeQueryBuilder.build()._toQuery();
    }

    public static Query rangeQuery(String field, String from, String to) {
        RangeQuery.Builder builder = rangeQueryBuilder(field);
        withFrom(builder, from);
        withTo(builder, to);

        return toQuery(builder);
    }

    public static Query rangeQuery(String field, long from, long to) {
        RangeQuery.Builder builder = rangeQueryBuilder(field);
        withFrom(builder, from);
        withTo(builder, to);

        return toQuery(builder);
    }

    /*
     * RegexQuery
     * */
    public static RegexpQuery.Builder regexQueryBuilder() {
        return new RegexpQuery.Builder();
    }

    public static void withFieldValue(RegexpQuery.Builder builder, String field, String value) {
        builder.field(field).value(value);
    }

    public static Query toQuery(RegexpQuery.Builder builder) {
        return builder.build()._toQuery();
    }

    public static Query regexQuery(String field, String value) {
        RegexpQuery.Builder builder = regexQueryBuilder();
        withFieldValue(builder, field, value);
        return toQuery(builder);
    }

    /*
     * WildcardQuery
     * */
    public static WildcardQuery.Builder wildcardQueryBuilder() {
        return new WildcardQuery.Builder();
    }

    public static void withFieldValue(WildcardQuery.Builder builder, String field, String value) {
        builder.field(field).value(value);
    }

    public static Query toQuery(WildcardQuery.Builder builder) {
        return builder.build()._toQuery();
    }

    public static Query wildcardQuery(String field, String value) {
        WildcardQuery.Builder builder = wildcardQueryBuilder();
        withFieldValue(builder, field, value);
        return toQuery(builder);
    }

    /*
     * ExistsQuery
     * */
    public static ExistsQuery.Builder existsQueryBuilder() {
        return new ExistsQuery.Builder();
    }

    public static ExistsQuery.Builder existsQueryBuilder(String field) {
        return new ExistsQuery.Builder().field(field);
    }

    public static Query toQuery(ExistsQuery.Builder builder) {
        return builder.build()._toQuery();
    }

    public static Query existsQuery(String field) {
        ExistsQuery.Builder builder = existsQueryBuilder(field);
        return toQuery(builder);
    }
}
