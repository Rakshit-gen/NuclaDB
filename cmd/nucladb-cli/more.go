package main

import (
	"context"
	"flag"
	"fmt"
	"strings"

	pb "github.com/Rakshit-gen/nucladb/proto/nucladbv1"
)

// withClient dials addr and runs fn with a timeout, closing the connection
// afterwards.
func withClient(addr string, fn func(context.Context, pb.NuclaDBClient) error) error {
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := ctxWithTimeout()
	defer cancel()
	return fn(ctx, pb.NewNuclaDBClient(conn))
}

// parseMetric maps "cosine", "l2" or "dot" to the enum; "" means the
// server's default.
func parseMetric(s string) (pb.DistanceMetric, error) {
	if s == "" {
		return pb.DistanceMetric_DISTANCE_METRIC_UNSPECIFIED, nil
	}
	if v, ok := pb.DistanceMetric_value["DISTANCE_METRIC_"+strings.ToUpper(s)]; ok {
		return pb.DistanceMetric(v), nil
	}
	return 0, fmt.Errorf("unknown metric %q (want cosine, l2 or dot)", s)
}

func metricName(m pb.DistanceMetric) string {
	return strings.ToLower(strings.TrimPrefix(m.String(), "DISTANCE_METRIC_"))
}

// parseWhere reads a -where clause: key:op or key:op:value, where value
// is comma-separated for in and not_in. Examples: year:gte:2024,
// team:in:search,ads, color:exists.
func parseWhere(s string) (*pb.MetadataFilter, error) {
	parts := strings.SplitN(s, ":", 3)
	if len(parts) < 2 {
		return nil, fmt.Errorf("-where %q: want key:op or key:op:value", s)
	}
	op, ok := pb.FilterOp_value["FILTER_OP_"+strings.ToUpper(parts[1])]
	if !ok {
		return nil, fmt.Errorf("-where %q: unknown op %q (want eq, ne, in, not_in, gt, gte, lt, lte, exists)", s, parts[1])
	}
	f := &pb.MetadataFilter{Key: parts[0], Op: pb.FilterOp(op)}
	if len(parts) == 3 {
		if f.Op == pb.FilterOp_FILTER_OP_IN || f.Op == pb.FilterOp_FILTER_OP_NOT_IN {
			f.Values = strings.Split(parts[2], ",")
		} else {
			f.Value = parts[2]
		}
	}
	return f, nil
}

func runGet(addr string, args []string) error {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	id := fs.String("id", "", "vector id (required)")
	tenant := fs.String("tenant", "", "tenant id (default: the reserved \"default\" tenant)")
	jsonOut := fs.Bool("json", false, "print machine-readable JSON")
	_ = fs.Parse(args)
	if *id == "" {
		return fmt.Errorf("get: -id is required")
	}
	return withClient(addr, func(ctx context.Context, c pb.NuclaDBClient) error {
		resp, err := c.Get(ctx, &pb.GetRequest{Id: *id, TenantId: *tenant})
		if err != nil {
			return err
		}
		v := resp.GetVector()
		if *jsonOut {
			return printJSON(map[string]any{"id": v.GetId(), "values": v.GetValues(), "metadata": v.GetMetadata()})
		}
		fmt.Printf("%s\t%v\t%v\n", v.GetId(), v.GetValues(), v.GetMetadata())
		return nil
	})
}

func runUpdateMetadata(addr string, args []string) error {
	fs := flag.NewFlagSet("update-metadata", flag.ExitOnError)
	id := fs.String("id", "", "vector id (required)")
	tenant := fs.String("tenant", "", "tenant id (default: the reserved \"default\" tenant)")
	var meta kvFlags
	fs.Var(&meta, "meta", "metadata key=value pair; repeatable. Replaces all existing metadata")
	_ = fs.Parse(args)
	if *id == "" {
		return fmt.Errorf("update-metadata: -id is required")
	}
	return withClient(addr, func(ctx context.Context, c pb.NuclaDBClient) error {
		if _, err := c.UpdateMetadata(ctx, &pb.UpdateMetadataRequest{Id: *id, Metadata: parseKV(meta), TenantId: *tenant}); err != nil {
			return err
		}
		fmt.Printf("updated id=%s\n", *id)
		return nil
	})
}

func runList(addr string, args []string) error {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	tenant := fs.String("tenant", "", "tenant id (default: the reserved \"default\" tenant)")
	pageSize := fs.Int("page-size", 0, "ids fetched per request (0 = server default)")
	_ = fs.Parse(args)
	return withClient(addr, func(ctx context.Context, c pb.NuclaDBClient) error {
		token := ""
		for {
			resp, err := c.List(ctx, &pb.ListRequest{TenantId: *tenant, PageToken: token, PageSize: int32(*pageSize)})
			if err != nil {
				return err
			}
			for _, id := range resp.GetIds() {
				fmt.Println(id)
			}
			if token = resp.GetNextPageToken(); token == "" {
				return nil
			}
		}
	})
}

func runCount(addr string, args []string) error {
	fs := flag.NewFlagSet("count", flag.ExitOnError)
	tenant := fs.String("tenant", "", "tenant id (default: the reserved \"default\" tenant)")
	_ = fs.Parse(args)
	return withClient(addr, func(ctx context.Context, c pb.NuclaDBClient) error {
		resp, err := c.Count(ctx, &pb.CountRequest{TenantId: *tenant})
		if err != nil {
			return err
		}
		fmt.Println(resp.GetCount())
		return nil
	})
}

func runTenants(addr string, args []string) error {
	fs := flag.NewFlagSet("tenants", flag.ExitOnError)
	jsonOut := fs.Bool("json", false, "print machine-readable JSON")
	_ = fs.Parse(args)
	return withClient(addr, func(ctx context.Context, c pb.NuclaDBClient) error {
		resp, err := c.ListTenants(ctx, &pb.ListTenantsRequest{})
		if err != nil {
			return err
		}
		if *jsonOut {
			return printJSON(resp.GetTenants())
		}
		for _, t := range resp.GetTenants() {
			fmt.Printf("%s\tvectors=%d\tdim=%d\tmetric=%s\tmax_vectors=%d\tmax_qps=%g\n",
				t.GetTenantId(), t.GetVectorCount(), t.GetDim(), metricName(t.GetMetric()),
				t.GetQuota().GetMaxVectors(), t.GetQuota().GetMaxQps())
		}
		return nil
	})
}

func runDeleteTenant(addr string, args []string) error {
	fs := flag.NewFlagSet("delete-tenant", flag.ExitOnError)
	id := fs.String("id", "", "tenant id (required)")
	_ = fs.Parse(args)
	if *id == "" {
		return fmt.Errorf("delete-tenant: -id is required")
	}
	return withClient(addr, func(ctx context.Context, c pb.NuclaDBClient) error {
		if _, err := c.DeleteTenant(ctx, &pb.DeleteTenantRequest{TenantId: *id}); err != nil {
			return err
		}
		fmt.Printf("deleted tenant %q\n", *id)
		return nil
	})
}

func runSetQuota(addr string, args []string) error {
	fs := flag.NewFlagSet("set-quota", flag.ExitOnError)
	id := fs.String("id", "", "tenant id (required)")
	maxVectors := fs.Int64("max-vectors", 0, "max vectors this tenant may hold (0 = unlimited)")
	maxQPS := fs.Float64("max-qps", 0, "max requests/sec for this tenant (0 = unlimited)")
	_ = fs.Parse(args)
	if *id == "" {
		return fmt.Errorf("set-quota: -id is required")
	}
	return withClient(addr, func(ctx context.Context, c pb.NuclaDBClient) error {
		if _, err := c.SetQuota(ctx, &pb.SetQuotaRequest{TenantId: *id, Quota: &pb.TenantQuota{MaxVectors: *maxVectors, MaxQps: *maxQPS}}); err != nil {
			return err
		}
		fmt.Printf("set quota for %q\n", *id)
		return nil
	})
}
