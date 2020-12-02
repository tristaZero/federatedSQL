/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package federated.sql.executor;

import org.apache.calcite.DataContext;
import org.apache.calcite.DataContext.Variable;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.util.Holder;

import java.util.*;

import static org.apache.calcite.sql.parser.SqlParser.DEFAULT_IDENTIFIER_MAX_LENGTH;

;

/**
 * Calcite raw executor.
 *
 * This executor skips Calcite JDBC driver and call the crude parse, optimize, execute API by itself.
 */
public final class CalciteRawExecutor {
    
    private final RelDataTypeFactory typeFactory;
    
    private final CalciteSchema schema;
    
    private final CalciteCatalogReader catalogReader;
    
    private final Config parserConfig;
    
    private final SqlValidator validator;
    
    private final SqlToRelConverter relConverter;
    
    private final Map<String, Object> internalParameters;
    
    public CalciteRawExecutor(final Properties connectionProps) {
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(connectionProps);
        typeFactory = new JavaTypeFactoryImpl();
        SchemaFactory factory = config.schemaFactory(SchemaFactory.class, null);
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
//        schema = rootSchema.add(config.schema(), new ReflectiveSchema(factory.create(rootSchema.plus(), config.schema(), getOperands(connectionProps))));
        Schema schema = factory.create(rootSchema.plus(), config.schema(), getOperands(connectionProps));
        rootSchema.add(config.schema(),schema);
        this.schema = rootSchema;
        catalogReader = new CalciteCatalogReader(this.schema, Collections.singletonList(config.schema()), typeFactory, config);
        parserConfig = SqlParser.config()
                .withLex(config.lex())
                .withIdentifierMaxLength(DEFAULT_IDENTIFIER_MAX_LENGTH)
                .withConformance(config.conformance())
                .withParserFactory(SqlParserImpl.FACTORY);
        validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), catalogReader, typeFactory, SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(config.lenientOperatorLookup())
                .withSqlConformance(config.conformance())
                .withDefaultNullCollation(config.defaultNullCollation())
                .withIdentifierExpansion(true));
        relConverter = createSqlToRelConverter();
        internalParameters = getInternalParameters(config);
    }
    
    private Map<String, Object> getOperands(final Properties properties) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (String each : properties.stringPropertyNames()) {
            if (each.startsWith("schema.")) {
                result.put(each.substring("schema.".length()), properties.getProperty(each));
            }
        }
        return result;
    }
    
    private SqlToRelConverter createSqlToRelConverter() {
        RelOptCluster cluster = newCluster();
        SqlToRelConverter.Config config = SqlToRelConverter.config().withTrimUnusedFields(true);
        ViewExpander expander = (rowType, queryString, schemaPath, viewPath) -> null;
        return new SqlToRelConverter(expander, validator, catalogReader, cluster, StandardConvertletTable.INSTANCE, config);
    }
    
    private RelOptCluster newCluster() {
        RelOptPlanner planner = new VolcanoPlanner();
        addPlanRules(planner);
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(typeFactory));
    }
    
    private void addPlanRules(final RelOptPlanner planner) {
        planner.addRule(CoreRules.PROJECT_TO_CALC);
        planner.addRule(CoreRules.FILTER_TO_CALC);
        planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
//        planner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_CALC_RULE);
//        planner.addRule(EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE);
//        planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE);
    }
    
    private Map<String, Object> getInternalParameters(final CalciteConnectionConfig config) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("_conformance", config.conformance());
        Holder<Long> timeHolder = Holder.of(System.currentTimeMillis());
        long time = timeHolder.get();
        TimeZone timeZone = TimeZone.getDefault();
        long localOffset = timeZone.getOffset(time);
        long currentOffset = localOffset;
        result.put(Variable.CURRENT_TIMESTAMP.camelName, time + currentOffset);
        result.put(Variable.LOCAL_TIMESTAMP.camelName, time + localOffset);
        result.put(Variable.UTC_TIMESTAMP.camelName, time);
        result.put(Variable.TIMEOUT.camelName, 1000L);
        return result;
    }
    /**
     * Execute.
     *
     * @return calcite query result
     * @throws SqlParseException SQL parse exception
     */
    public Enumerable<Object[]> execute(final String sql) throws SqlParseException {
        SqlNode sqlNode = SqlParser.create(sql, parserConfig).parseQuery();
        SqlNode validNode = validator.validate(sqlNode);
        RelNode logicPlan = relConverter.convertQuery(validNode, false, true).rel;
//        System.out.println(RelOptUtil.dumpPlan("[Logical plan]", logicPlan, SqlExplainFormat.TEXT, SqlExplainLevel.NON_COST_ATTRIBUTES));
        RelNode bestPlan = optimize(logicPlan);
//        System.out.println(RelOptUtil.dumpPlan("[Physical plan]", bestPlan, SqlExplainFormat.TEXT, SqlExplainLevel.NON_COST_ATTRIBUTES));
        return execute(bestPlan);
    }
    
    private RelNode optimize(final RelNode logicPlan) {
        RelOptPlanner planner = relConverter.getCluster().getPlanner();
        planner.setRoot(planner.changeTraits(logicPlan, relConverter.getCluster().traitSet().replace(EnumerableConvention.INSTANCE)));
        return planner.findBestExp();
    }
    
    private Enumerable<Object[]> execute(final RelNode bestPlan) {
        Bindable<Object[]> executablePlan = EnumerableInterpretable.toBindable(internalParameters, null, (EnumerableRel) bestPlan, EnumerableRel.Prefer.ARRAY);
        return executablePlan.bind(createDataContext());
    }

    private DataContext createDataContext() {
        return new DataContext() {
            
            @Override
            public SchemaPlus getRootSchema() {
                return schema.plus();
            }
    
            @Override
            public JavaTypeFactory getTypeFactory() {
                return (JavaTypeFactory) typeFactory;
            }
    
            @Override
            public QueryProvider getQueryProvider() {
                return null;
            }
    
            @Override
            public Object get(String name) {
                return internalParameters.get(name);
            }
        };
    }
}
