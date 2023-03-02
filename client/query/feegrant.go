package query

import (
	"errors"
	"fmt"
	"time"

	"github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/query"
	"github.com/cosmos/cosmos-sdk/x/feegrant"
	"github.com/strangelove-ventures/lens/client"
)

// Feegrant_GrantsByGranterRPC returns all requested grants for the given Granter.
// Default behavior will return all grants.
func Feegrant_GrantsByGranterRPC(q *Query, address string) ([]*feegrant.Grant, error) {
	grants := []*feegrant.Grant{}
	paginator := &query.PageRequest{}
	allPages := true

	if q.Options.Pagination != nil {
		paginator = q.Options.Pagination
		allPages = false
	}

	req := &feegrant.QueryAllowancesByGranterRequest{Granter: address, Pagination: paginator}
	queryClient := feegrant.NewQueryClient(q.Client)
	ctx, cancel := q.GetQueryContext()
	defer cancel()
	hasNextPage := true

	for {
		res, err := queryClient.AllowancesByGranter(ctx, req)
		if err != nil {
			return nil, err
		}

		fmt.Printf("feegrant query ctx: %+v\n", ctx)

		if res.Allowances != nil {
			grants = append(grants, res.Allowances...)
		}

		if res.Pagination != nil {
			req.Pagination.Key = res.Pagination.NextKey
			if len(res.Pagination.NextKey) == 0 {
				hasNextPage = false
			}
		} else {
			hasNextPage = false
		}

		if !allPages || !hasNextPage {
			break
		}
	}

	return grants, nil
}

// Feegrant_GrantsByGranteeRPC returns all requested grants for the given grantee.
// Default behavior will return all grants.
func Feegrant_GrantsByGranteeRPC(q *Query, address string) ([]*feegrant.Grant, error) {
	grants := []*feegrant.Grant{}
	paginator := &query.PageRequest{}
	allPages := true

	if q.Options.Pagination != nil {
		paginator = q.Options.Pagination
		allPages = false
	}

	req := &feegrant.QueryAllowancesRequest{Grantee: address, Pagination: paginator}
	queryClient := feegrant.NewQueryClient(q.Client)
	ctx, cancel := q.GetQueryContext()
	defer cancel()
	hasNextPage := true

	for {
		res, err := queryClient.Allowances(ctx, req)
		if err != nil {
			return nil, err
		}

		fmt.Printf("feegrant query ctx: %+v\n", ctx)

		if res.Allowances != nil {
			grants = append(grants, res.Allowances...)
		}

		if res.Pagination != nil {
			req.Pagination.Key = res.Pagination.NextKey
			if len(res.Pagination.NextKey) == 0 {
				hasNextPage = false
			}
		} else {
			hasNextPage = false
		}

		if !allPages || !hasNextPage {
			break
		}
	}

	return grants, nil
}

// Searches for valid, existing BasicAllowance grants for the given grantee & ChainClient's configured granter.
// Expired grants are ignored. Other grant types are ignored.
func GetGranteeValidBasicGrants(cc *client.ChainClient, granteeKey string) ([]*feegrant.Grant, error) {
	validGrants := []*feegrant.Grant{}

	if cc.Config.FeeGrants == nil {
		return nil, errors.New("no feegrant configuration for chainclient")
	}

	granterAddr, err := cc.AccountFromKeyOrAddress(cc.Config.FeeGrants.GranterKey)
	if err != nil {
		return nil, err
	}
	granterEncodedAddr := cc.MustEncodeAccAddr(granterAddr)

	address, err := cc.AccountFromKeyOrAddress(granteeKey)
	if err != nil {
		return nil, err
	}

	options := QueryOptions{}
	q := &Query{Client: cc, Options: &options}
	encodedAddr := cc.MustEncodeAccAddr(address)
	grants, err := Feegrant_GrantsByGranteeRPC(q, encodedAddr)
	if err != nil {
		return nil, err
	}

	for _, grant := range grants {
		if grant.Granter == granterEncodedAddr {
			switch grant.Allowance.TypeUrl {
			case "/cosmos.feegrant.v1beta1.BasicAllowance":
				var feegrantAllowance feegrant.FeeAllowanceI
				e := cc.Codec.InterfaceRegistry.UnpackAny(grant.Allowance, &feegrantAllowance)
				if e != nil {
					return nil, e
				}
				if isValidGrant(feegrantAllowance.(*feegrant.BasicAllowance)) {
					validGrants = append(validGrants, grant)
				}
			default:
				fmt.Printf("Ignoring grant type %s for granter %s and grantee %s\n", grant.Allowance.TypeUrl, grant.Granter, grant.Grantee)
			}
		}
	}

	return validGrants, nil
}

// Searches for valid, existing BasicAllowance grants for the ChainClient's configured Feegranter.
// Expired grants are ignored. Other grant types are ignored.
func GetValidBasicGrants(cc *client.ChainClient) ([]*feegrant.Grant, error) {
	validGrants := []*feegrant.Grant{}

	if cc.Config.FeeGrants == nil {
		return nil, errors.New("no feegrant configuration for chainclient")
	}

	keyNameOrAddress := cc.Config.FeeGrants.GranterKey
	address, err := cc.AccountFromKeyOrAddress(keyNameOrAddress)
	if err != nil {
		return nil, err
	}

	options := QueryOptions{}
	q := &Query{Client: cc, Options: &options}
	encodedAddr := cc.MustEncodeAccAddr(address)
	grants, err := Feegrant_GrantsByGranterRPC(q, encodedAddr)
	if err != nil {
		return nil, err
	}

	for _, grant := range grants {
		switch grant.Allowance.TypeUrl {
		case "/cosmos.feegrant.v1beta1.BasicAllowance":
			//var feegrantAllowance feegrant.BasicAllowance
			var feegrantAllowance feegrant.FeeAllowanceI
			e := cc.Codec.InterfaceRegistry.UnpackAny(grant.Allowance, &feegrantAllowance)
			if e != nil {
				return nil, e
			}
			//feegrantAllowance := grant.Allowance.GetCachedValue().(*feegrant.BasicAllowance)
			if isValidGrant(feegrantAllowance.(*feegrant.BasicAllowance)) {
				validGrants = append(validGrants, grant)
			}
		default:
			fmt.Printf("Ignoring grant type %s for granter %s and grantee %s\n", grant.Allowance.TypeUrl, grant.Granter, grant.Grantee)
		}
	}

	return validGrants, nil
}

// True if the grant has not expired and all coins have positive balances, false otherwise
// Note: technically, any single coin with a positive balance makes the grant usable
func isValidGrant(a *feegrant.BasicAllowance) bool {
	//grant expired due to time limit
	if a.Expiration != nil && time.Now().After(*a.Expiration) {
		return false
	}

	//feegrant without a spending limit specified allows unlimited fees to be spent
	valid := true

	//spending limit is specified, check if there are funds remaining on every coin
	if a.SpendLimit != nil {
		for _, coin := range a.SpendLimit {
			if coin.Amount.LTE(types.ZeroInt()) {
				valid = false
			}
		}
	}

	return valid
}
