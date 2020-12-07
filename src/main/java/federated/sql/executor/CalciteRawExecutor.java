package federated.sql.executor;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.interpreter.InterpretableConvention;
import org.apache.calcite.interpreter.InterpretableConverter;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Bindable;
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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

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
    
    public CalciteRawExecutor(final Properties connectionProps) {
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(connectionProps);
        typeFactory = new JavaTypeFactoryImpl();
        SchemaFactory factory = config.schemaFactory(SchemaFactory.class, null);
        schema = CalciteSchema.createRootSchema(true);
        schema.add(config.schema(), factory.create(schema.plus(), config.schema(), getOperands(connectionProps)));
        catalogReader = new CalciteCatalogReader(schema, Collections.singletonList(config.schema()), typeFactory, config);
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
//        return executeEnumerableInterpretable(bestPlan);
        return executeByCustomInterpretable(bestPlan);
    }
    
    private RelNode optimize(final RelNode logicPlan) {
        RelOptPlanner planner = relConverter.getCluster().getPlanner();
        planner.setRoot(planner.changeTraits(logicPlan, relConverter.getCluster().traitSet().replace(EnumerableConvention.INSTANCE)));
        return planner.findBestExp();
    }
    
    private Enumerable<Object[]> executeEnumerableInterpretable(final RelNode bestPlan) {
        Bindable<Object[]> executablePlan = EnumerableInterpretable.toBindable(Collections.emptyMap(), null, (EnumerableRel) bestPlan, EnumerableRel.Prefer.ARRAY);
        return executablePlan.bind(createDataContext());
    }
    
    private Enumerable<Object[]> executeByCustomInterpretable(final RelNode bestPlan) {
        RelOptCluster cluster = relConverter.getCluster();
        return new CustomInterpretableConverter(cluster, cluster.traitSetOf(InterpretableConvention.INSTANCE), bestPlan).bind(createDataContext());
    }
    
    public static final class CustomInterpretableConverter extends InterpretableConverter {
    
        public CustomInterpretableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
            super(cluster, traits, input);
        }
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
                return null;
            }
        };
    }
}
