package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	pb "github.com/Rakshit-gen/nucladb/proto/nucladbv1"
)

func runCreateKey(addr string, args []string) error {
	fs := flag.NewFlagSet("create-key", flag.ExitOnError)
	name := fs.String("name", "", "label to recognize the key by in \"keys\"")
	tenants := fs.String("tenants", "", "comma-separated tenants the key may use (\"*\" for all; required unless -admin)")
	admin := fs.Bool("admin", false, "make an admin key, which can manage tenants and keys")
	ttl := fs.Duration("ttl", 0, "expire the key after this long, e.g. 720h (0 = never)")
	jsonOut := fs.Bool("json", false, "print machine-readable JSON")
	_ = fs.Parse(args)
	var list []string
	if *tenants != "" {
		list = strings.Split(*tenants, ",")
	}
	if !*admin && len(list) == 0 {
		return fmt.Errorf("create-key: -tenants is required unless -admin is set")
	}
	return withClient(addr, func(ctx context.Context, c pb.NuclaDBClient) error {
		resp, err := c.CreateAPIKey(ctx, &pb.CreateAPIKeyRequest{
			Name: *name, Tenants: list, Admin: *admin, TtlSeconds: int64(ttl.Seconds()),
		})
		if err != nil {
			return err
		}
		if *jsonOut {
			return printJSON(resp)
		}
		fmt.Println(resp.GetKey())
		fmt.Fprintf(os.Stderr, "created %s; save the key above now, the server can't show it again\n", resp.GetInfo().GetId())
		return nil
	})
}

func runKeys(addr string, args []string) error {
	fs := flag.NewFlagSet("keys", flag.ExitOnError)
	jsonOut := fs.Bool("json", false, "print machine-readable JSON")
	_ = fs.Parse(args)
	return withClient(addr, func(ctx context.Context, c pb.NuclaDBClient) error {
		resp, err := c.ListAPIKeys(ctx, &pb.ListAPIKeysRequest{})
		if err != nil {
			return err
		}
		if *jsonOut {
			return printJSON(resp.GetKeys())
		}
		for _, k := range resp.GetKeys() {
			expires := "never"
			if k.GetExpiresUnix() != 0 {
				expires = time.Unix(k.GetExpiresUnix(), 0).UTC().Format(time.RFC3339)
			}
			fmt.Printf("%s\tname=%q\ttenants=%s\tadmin=%v\tcreated=%s\texpires=%s\n",
				k.GetId(), k.GetName(), strings.Join(k.GetTenants(), ","), k.GetAdmin(),
				time.Unix(k.GetCreatedUnix(), 0).UTC().Format(time.RFC3339), expires)
		}
		return nil
	})
}

func runRevokeKey(addr string, args []string) error {
	fs := flag.NewFlagSet("revoke-key", flag.ExitOnError)
	id := fs.String("id", "", "key id from \"keys\", like key_0123456789abcdef (required)")
	_ = fs.Parse(args)
	if *id == "" {
		return fmt.Errorf("revoke-key: -id is required")
	}
	return withClient(addr, func(ctx context.Context, c pb.NuclaDBClient) error {
		if _, err := c.RevokeAPIKey(ctx, &pb.RevokeAPIKeyRequest{Id: *id}); err != nil {
			return err
		}
		fmt.Printf("revoked %s\n", *id)
		return nil
	})
}
