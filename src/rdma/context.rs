//! RDMA device context wrapper.
//!
//! Provides safe access to an opened RDMA device via `ibv_context`.

use std::ffi::CStr;

use ibverbs_sys::*;

use crate::error::RdmaError;

/// Opened RDMA device context.
///
/// Created via [`Context::open`] and automatically closed on drop via
/// `ibv_close_device`.
pub struct Context {
    /// Raw ibv_context pointer (non-null)
    inner: *mut ibv_context,
    /// Cached device name
    name: String,
}

impl Context {
    /// Open the first available RDMA device.
    ///
    /// Returns `None` if no RDMA devices are found on the system.
    pub fn open() -> Option<Self> {
        let mut num_devices: libc::c_int = 0;

        let device_list = unsafe { ibv_get_device_list(&mut num_devices) };

        if device_list.is_null() || num_devices == 0 {
            if !device_list.is_null() {
                unsafe { ibv_free_device_list(device_list) };
            }
            return None;
        }

        let device = unsafe { *device_list };
        if device.is_null() {
            unsafe { ibv_free_device_list(device_list) };
            return None;
        }

        let ctx = unsafe { ibv_open_device(device) };
        // Free the device list as soon as we've opened the device
        unsafe { ibv_free_device_list(device_list) };

        if ctx.is_null() {
            return None;
        }

        // Cache the device name
        let name = unsafe {
            let name_ptr = ibv_get_device_name(device);
            if name_ptr.is_null() {
                ibv_close_device(ctx);
                return None;
            }
            CStr::from_ptr(name_ptr).to_string_lossy().into_owned()
        };

        Some(Context { inner: ctx, name })
    }

    /// Get the device name (e.g., "mlx5_0").
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Query device attributes.
    ///
    /// Returns cached device attributes such as firmware version, node GUID,
    /// and vendor ID.
    pub fn query_device(&self) -> Result<DeviceAttr, RdmaError> {
        let mut attr: ibv_device_attr = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };

        let ret = unsafe { ibv_query_device(self.inner, &mut attr) };
        if ret != 0 {
            return Err(RdmaError::HardwareError(format!(
                "ibv_query_device failed with return code {}",
                ret
            )));
        }

        let fw_ver = unsafe { CStr::from_ptr(attr.fw_ver.as_ptr() as *const libc::c_char) }
            .to_string_lossy()
            .into_owned();

        Ok(DeviceAttr {
            fw_ver,
            node_guid: attr.node_guid,
            vendor_id: attr.vendor_id,
            vendor_part_id: attr.vendor_part_id,
            hw_ver: attr.hw_ver,
            max_qp: attr.max_qp,
            max_qp_wr: attr.max_qp_wr,
            max_cq: attr.max_cq,
            max_cqe: attr.max_cqe,
            max_mr: attr.max_mr,
            max_pd: attr.max_pd,
            max_qp_rd_atom: attr.max_qp_rd_atom,
            max_sge: attr.max_sge,
            phys_port_cnt: attr.phys_port_cnt,
        })
    }

    /// Query port attributes for the given port number.
    pub fn query_port(&self, port_num: u8) -> Result<PortAttr, RdmaError> {
        let mut attr: ibv_port_attr = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };

        let ret = unsafe { ibv_query_port_attr(self.inner, port_num, &mut attr) };
        if ret != 0 {
            return Err(RdmaError::HardwareError(format!(
                "ibv_query_port failed with return code {}",
                ret
            )));
        }

        Ok(PortAttr {
            lid: attr.lid,
            state: attr.state,
            max_mtu: attr.max_mtu,
            active_mtu: attr.active_mtu,
            port_cap_flags: attr.port_cap_flags,
            max_msg_sz: attr.max_msg_sz,
            link_layer: attr.link_layer,
        })
    }

    /// Get the raw `ibv_context` pointer for use in FFI calls.
    ///
    /// # Safety
    ///
    /// The returned pointer is valid as long as this `Context` exists.
    pub fn as_ptr(&self) -> *mut ibv_context {
        self.inner
    }

    /// Query the GID (Global Identifier) for a given port and GID index.
    ///
    /// Required for RoCE (RDMA over Converged Ethernet) connections.
    /// Returns `None` if `ibv_query_gid` fails.
    pub fn query_gid(&self, port_num: u8, gid_index: i32) -> Option<ibv_gid> {
        let mut gid: ibv_gid = unsafe { std::mem::zeroed() };
        let ret = unsafe { ibv_query_gid(self.inner, port_num, gid_index, &mut gid) };
        if ret != 0 {
            return None;
        }
        Some(gid)
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        unsafe {
            ibv_close_device(self.inner);
        }
    }
}

// SAFETY: ibv_context can be sent and shared across threads (per libibverbs docs).
unsafe impl Send for Context {}
unsafe impl Sync for Context {}

/// Safe Rust representation of `ibv_device_attr` fields we care about.
#[derive(Debug, Clone)]
pub struct DeviceAttr {
    pub fw_ver: String,
    pub node_guid: u64,
    pub vendor_id: u32,
    pub vendor_part_id: u32,
    pub hw_ver: u32,
    pub max_qp: libc::c_int,
    pub max_qp_wr: libc::c_int,
    pub max_cq: libc::c_int,
    pub max_cqe: libc::c_int,
    pub max_mr: libc::c_int,
    pub max_pd: libc::c_int,
    pub max_qp_rd_atom: libc::c_int,
    pub max_sge: libc::c_int,
    pub phys_port_cnt: u8,
}

/// Safe Rust representation of `ibv_port_attr` fields we care about.
#[derive(Debug, Clone)]
pub struct PortAttr {
    pub lid: u16,
    pub state: ibv_port_state,
    pub max_mtu: ibv_mtu,
    pub active_mtu: ibv_mtu,
    pub port_cap_flags: u32,
    pub max_msg_sz: u32,
    pub link_layer: u8,
}
