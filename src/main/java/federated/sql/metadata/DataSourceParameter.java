package federated.sql.metadata;

import lombok.Getter;
import lombok.Setter;

/**
 * DataSourceParameter.
 */
@Getter
@Setter
public final class DataSourceParameter {
    
    private String url;
    
    private String username;
    
    private String password;
    
    private String diver;
}
