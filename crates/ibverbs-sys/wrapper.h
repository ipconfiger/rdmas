/// Wrapper header for bindgen: only include what we need.
/// We do NOT include ib_user_ioctl_verbs.h (which contains
/// problematic anonymous unions) — we only need the core
/// verbs API from verbs.h

#include <infiniband/verbs.h>
