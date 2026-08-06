package conf

import "testing"

func TestValidateBattleDepartureConfig(t *testing.T) {
t.Run("production requires locator", func(t *testing.T) {
cfg := Config{Mode: ModeAgones}
cfg.DSAuth.AuthorityMode = "redis"
if err := cfg.ValidateBattleDepartureConfig(); err == nil {
t.Fatal("production without locator must fail closed")
}
})

t.Run("locator configured passes", func(t *testing.T) {
cfg := Config{Mode: ModeAgones, LocatorAddr: "player-locator:20006"}
cfg.DSAuth.AuthorityMode = "redis"
if err := cfg.ValidateBattleDepartureConfig(); err != nil {
t.Fatalf("locator configured rejected: %v", err)
}
})
}

func TestValidateAllocationAbortAuthConfig(t *testing.T) {
base := Config{}
base.DSAuth.AuthorityMode = "redis"
base.DSAuth.Secret = "ds-callback-key-is-at-least-32-bytes-long"
base.Allocator.AllocationAbortAuthSecret = "04b47e36-a832-4529-87b0-e0407b6225ef"
base.Allocator.AllocationAbortAuthAudience = "ds-allocator:battle-allocation-abort"
if err := base.ValidateAllocationAbortAuthConfig(); err != nil {
t.Fatalf("valid dedicated auth rejected: %v", err)
}
t.Run("ds-callback", func(t *testing.T) {
cfg := base
cfg.Allocator.AllocationAbortAuthSecret = base.DSAuth.Secret
if err := cfg.ValidateAllocationAbortAuthConfig(); err == nil {
t.Fatal("allocation abort key reused ds-callback trust domain")
}
})
cfg := base
cfg.Allocator.AllocationAbortAuthAudience = ""
if err := cfg.ValidateAllocationAbortAuthConfig(); err == nil {
t.Fatal("empty allocation abort audience accepted")
}
}
