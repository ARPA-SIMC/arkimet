from arkimet.formatter import Formatter

cosmo_centres = {78, 80, 200}

# prodtab dict generated by a fortran program including literal cosmo code
prodtab = {
    201: {
        5: "PABS_RAD surface photosynthetic active radiation W m-2",
        13: "SOHR_RAD solar radiation heating rate in the atmosphere K s-1",
        14: "THHR_RAD thermal radiation heating rate in the atmosphere K s-1",
        18: "ALHFL_BS averaged latent heat flux from bare soil evaporation W m-2",
        19: "ALHFL_PL averaged latent heat flux from plants W m-2",
        20: "DURSUN duration of sunshine s",
        21: "RSTOM stomata resistance s/m",
        22: "SWDIRS_RAD direct downward shortwave radiation at the surface W m-2",
        23: "SWDIFDS_RAD diffuse downward sw radiation at the surface W m-2",
        24: "SWDIFUS_RAD diffuse upward sw radiation at the surface W m-2",
        25: "THDS_RAD downward lw radiation at the surface W m-2",
        26: "THUS_RAD upward lw radiation at the surface W m-2",
        27: "SODT_RAD solar downward radiation at top W m-2",
        28: "ASWDIR_SN averaged direct downward sw radiation on a plane directed normal to the sun W m-2",
        29: "CLC cloud area fraction 1",
        30: "CLC_SGS subgrid scale cloud area fraction 1",
        31: "QC specific cloud liquid water content kg kg-1",
        33: "QI specific cloud ice content kg kg-1",
        35: "QR specific rain content kg kg-1",
        36: "QS specific snow content kg kg-1",
        37: "TQR total rain water content vertically integrated kg m-2",
        38: "TQS total snow content vertically integrated kg m-2",
        39: "QG specific graupel content kg kg-1",
        40: "TQG total graupel content vertically integrated kg m-2",
        41: "TWATER total water content kg m-2",
        42: "TDIV_HUM atmosphere water divergence kg m-2",
        43: "QC_RAD sub scale specific cloud liquid water content kg kg-1",
        44: "QI_RAD sub scale specific cloud ice content kg kg-1",
        48: "TCOND_MAX vertically integrated hydrometeors track (max) over last output interval kg/m2",
        49: "TCOND10_MX vertically integrated hydrometeors below -10 Celsius track (max) over last outpu kg/m2",
        57: "DQVDT_CONV convective tendency of water vapor s-1",
        58: "HBAS_SC height of shallow convective cloud base m",
        59: "HTOP_SC height of shallow convective cloud top m",
        61: "CLW_CON convective cloud liquid water 1",
        68: "HBAS_CON height of convective cloud base m",
        69: "HTOP_CON height of convective cloud top m",
        72: "BAS_CON index of convective cloud base 1",
        73: "TOP_CON index of convective cloud top 1",
        74: "DT_CON convective tendency of temperature K s-1",
        75: "DQV_CON convective tendency of specific humidity s-1",
        78: "DU_CON convective tendency of u-wind component m s-2",
        79: "DV_CON convective tendency of v-wind component m s-2",
        82: "HTOP_DC height of dry convection top m",
        84: "HZEROCL height of freezing level m",
        85: "SNOWLMT height of the snow fall limit in m above sea level m",
        88: "DQC_CON convective tendency of specific cloud water s-1",
        89: "DQI_CON convective tendency of specific cloud ice s-1",
        90: "TTDIAB_CONV convective tendency of diabatic temperature K s-1",
        91: "C_T_LK shape factor of temperature profile in lake thermocline 1",
        92: "GAMSO_LK attenuation coefficient for solar radiation in lake water m-1",
        93: "DP_BS_LK thickness of thermally active layer of bottom sediments m",
        94: "H_B1_LK thickness of the upper layer of bottom sediments m",
        95: "H_ML_LK thickness of mixed layer m",
        96: "DEPTH_LK lake depth m",
        97: "FETCH_LK wind fetch over lake m",
        99: "QRS precipitation water (water loading) 1",
        100: "PRR_GSP mass flux density of large scale rainfall kg m-2 s-1",
        101: "PRS_GSP mass flux density of large scale snowfall kg m-2 s-1",
        102: "RAIN_GSP large scale rainfall kg m-2",
        111: "PRR_CON mass flux density of convective rainfall kg m-2 s-1",
        112: "PRS_CON mass flux density of convective snowfall kg m-2 s-1",
        113: "RAIN_CON convective rainfall kg m-2",
        123: "TOT_SNOW total snowfall amount kg m-2",
        129: "FRESHSNW freshness of snow 1",
        131: "PRG_GSP mass flux density of large scale graupel kg m-2 s-1",
        132: "GRAU_GSP large scale graupel kg m-2",
        133: "RHO_SNOW density of snow kg m-3",
        134: "PRH_GSP mass flux density of large scale hail kg m-2 s-1",
        135: "HAIL_GSP large scale hail kg m-2",
        136: "TQH total hail content vertically integrated kg m-2",
        137: "WLIQ_SNOW_M snow liquid water content m",
        139: "PP deviation from reference pressure hPa",
        140: "RCLD standard deviation of saturation deficit -",
        141: "SDI_1 Supercell detection index 1 (rotating up-/downdrafts 1/s",
        142: "SDI_2 Supercell detection index 2 (rotating updrafts, only 1/s",
        143: "CAPE_MU cape of most unstable parcel J kg-1",
        144: "CIN_MU convective inhibition of most unstable parcel J kg-1",
        145: "CAPE_ML cape of mean surface layer parcel J kg-1",
        146: "CIN_ML convective inhibition of mean surface layer parcel J kg-1",
        147: "TKE_CON convective turbulent kinetic energy J kg-1",
        148: "TKETENS tendency of turbulent kinetic energy m s-1",
        151: "EDR dissipation rate of turbulent kinetic energy m2 s-3",
        152: "TKE turbulent kinetic energy m2 s-2",
        153: "TKVM diffusion coefficient of momentum m2 s-1",
        154: "TKVH diffusion coefficient of heat m2 s-1",
        155: "DTKE_SSO TKE tendency due to SSO wakes m2 s-3",
        156: "DTKE_HSH TKE tendency due to horizontal shear m2 s-3",
        157: "DTKE_CON TKE tendency due to convective buoyancy m2 s-3",
        158: "TKHM horizontal diffusion coefficient of momentum m2 s-1",
        159: "TKHH horizontal diffusion coefficient of heat m2 s-1",
        170: "TCM drag coefficient of momentum 1",
        171: "TCH drag coefficient of heat 1",
        187: "VMAX_10M maximum 10m wind speed m s-1",
        190: "T_BS_LK climatological temperature at bottom of thermally active layer of sediments K",
        191: "T_BOT_LK temperature at water bottom sediment interface K",
        192: "T_B1_LK temperature at bottom of upper layer of sediments K",
        193: "T_WML_LK mixed layer temperature K",
        194: "T_MNW_LK mean temperature of water column K",
        196: "LPI Lightning Potential Index J kg-1",
        197: "T_SO soil temperature K",
        198: "W_SO soil water content m",
        199: "W_SO_ICE soil frozen water content m",
        200: "W_I canopy water amount m",
        202: "SNOW_C snow fraction %",
        203: "T_SNOW snow surface temperature K",
        212: "RSMIN minimum stomata resistance s m-1",
        215: "T_ICE temperature of ice upper surface K",
        216: "VABSMX_10M maximum 10m wind speed without gust m s-1",
        218: "VGUST_DYN_10M maximum 10m dynamical gust m s-1",
        219: "VGUST_CON_10M maximum 10m convective gust m s-1",
        220: "ASWDIR_SNO averaged direct downward sw radiation on a plane directed normal to the sun (exc W m-2",
        230: "DBZ unattenuated radar reflectivity in Rayleigh approximation 1",
        240: "MFLX_CON convective mass flux density kg m-2 s-1",
        241: "CAPE_CON specific convectively available potential energy J kg-1",
        242: "MCONV Horizontal low-level moisture convergence (1km-layer-average) kg/(kg*s)",
        243: "QCVG_CON moisture convergence in the air for kuo type closure s-1",
    },
    202: {
        32: "TINC_LH temperature increment due to latent heat K",
        33: "FR_PAVED ground fraction covered by sealed/paved surfaces 1",
        34: "AHF Yearly averaged anthropogenic heat flux by traffic and heating 1",
        35: "TTENS_DIAB temperature tendency due to pure diabatic processes K",
        36: "ECHOTOP Minimum pressure of ocurrence of reflectivity above a threshold, min over last o hPa",
        37: "ECHOTOPinM Maximum height above sea of ocurrence of reflectivity above a threshold, max ove m",
        38: "SKT skin temperature K",
        39: "SKC skin conductivity W m-2 K-1",
        43: "DT_SSO tendency of t due to SSO K s-1",
        44: "DU_SSO tendency of u due to SSO m s-2",
        45: "DV_SSO tendency of v due to SSO m s-2",
        46: "SSO_STDH standard deviation of sub-grid scale orography m",
        47: "SSO_GAMMA anisotropy of sub-grid scale orography -",
        48: "SSO_THETA angle between principal axis of orography and east rad",
        49: "SSO_SIGMA mean slope of sub-grid scale orography -",
        55: "FR_LAKE fraction of inland lake water 1",
        56: "EMIS_RAD thermal surface emissivity -",
        57: "SOILTYP soil type 1",
        61: "LAI leaf area index 1",
        62: "ROOTDP root depth m",
        64: "HMO3 air pressure at ozone maximum Pa",
        65: "VIO3 vertical integrated ozone amount Pa",
        67: "PLCOV_MX vegetation area fraction maximum 1",
        68: "PLCOV_MN vegetation area fraction minimum 1",
        69: "LAI_MX leaf area index maximum 1",
        70: "LAI_MN leaf area index minimum 1",
        75: "FOR_E ground fraction covered by evergreen forest -",
        76: "FOR_D ground fraction covered by deciduous forest -",
        84: "AER_SO4 aerosol sulfat 1",
        86: "AER_DUST aerosol dust 1",
        91: "AER_ORG aerosol organic 1",
        92: "AER_BC aerosol black carbon 1",
        93: "AER_SS aerosol sea salt 1",
        94: "TWATFLXU total zonal water flux kg m-1 s-1",
        95: "TWATFLXV total medirional water flux kg m-1 s-1",
        96: "HORIZON horizon angle - topography ",
        97: "SWDIR_COR topo correction of direct solar radiation 1",
        98: "SLO_ANG slope angle - topography ",
        99: "SLO_ASP slope aspect - topography ",
        100: "SKYVIEW sky-view factor 1",
        104: "DQVDT tendency of water vapor s-1",
        105: "QVSFLX surface flux of water vapour s-1m-2",
        113: "FC coriolis parameter s-1",
        114: "RLAT latitude radian",
        115: "RLON longitude radian",
        121: "ZTD integrated total atmospheric refractivity ",
        122: "ZWD integrated wet atmospheric refractivity ",
        123: "ZHD integrated dry atmospheric refractivity ",
        127: "ALB_DRY dry soil albedo 1",
        128: "ALB_SAT saturated soil albedo 1",
        129: "ALB_DIF diffuse solar albedo 1",
        133: "VORTIC_U relative vorticity, u-component in rotated grid 1/s",
        134: "VORTIC_V relative vorticity, v-component in rotated grid 1/s",
        180: "O3 ozone mass mixing ratio kg kg-1",
        231: "USTR_SSO u-stress due to SSO N m-2",
        232: "VSTR_SSO v-stress due to SSO N m-2",
        233: "VDIS_SSO vertical integrated dissipation of kinetic energy due to SSO W m-2",
        234: "SOBS_RAD_CS clear-sky surface net downward shortwave radiation W m-2",
        235: "THBS_RAD_CS clear-sky surface net downward longwave radiation W m-2",
        250: "SUN_EL angle between the line of sight to the sun and the local horizont degree",
        251: "SUN_AZI angle between the line of sight to the sun and reference direction degree",
        252: "TURB_INTENS_MAX turbulence intensity 1",
    },
    203: {
        35: "UH_MAX updraft helicity track (max amplitude) over last output interval J/kg",
        36: "VORW_CTMAX rotation track (max amplitude) over last output interval 1/s",
        37: "W_CTMAX updraft track (max) over last output interval m/s",
        43: "WSOIL_FLX soil water flux between layers 1",
        44: "Q_ROFF ground water runoff, aquifer m",
        45: "WT_DEPTH depth of the water table 1",
        46: "W_SO_SL soil moisture saturation level 1",
        47: "W_SO_SAT soil moisture saturation 1",
        50: "TKET_ADV pure advective tendency of turbulent kinetic energy m s-1",
        135: "LCL_ML lifting condensation level of mean layer parcel m",
        136: "LFC_ML level of free convection of mean layer parcel m",
        137: "CAPE_3KM CAPE of mean layer parcel in the lowest 3 km J kg-1",
        138: "SWISS00 showalter index with windshear K",
        139: "SWISS12 surface lifted index with windshear K",
        147: "SLI surface lifted index K",
        149: "SI showalter index K",
        155: "BRN Bulk Richardson Number 1",
        156: "HPBL Height of boundary layer m",
        157: "CEILING Cloud ceiling height (above MSL) m",
        203: "CLDEPTH normalized cloud depth 1",
        204: "CLCT_MOD modified_total_cloud_cover 1",
    },
    204: {
        21: "FF_ANAI wind speed analysis increment m s-1",
        22: "DD_ANAI wind direction analysis increment degree",
        23: "T_ANAI temperature analysis increment K",
        24: "FI_ANAI geopotential analysis increment m2 s-2",
        25: "P_ANAI pressure analysis increment Pa",
        26: "PMSL_ANAI msl pressure analysis increment Pa",
        27: "QV_ANAI specific humidity analysis increment kg kg-1",
        28: "QC_ANAI specific cloud water content analysis increment kg kg-1",
        29: "TQV_ANAI precipitable water analysis increment kg m-2",
        30: "TQC_ANAI cloud water analysis increment kg m-2",
    },
    205: {
        1: "SYNME5 synthetic satellite images Meteosat5 -",
        2: "SYNME6 synthetic satellite images Meteosat6 -",
        3: "SYNME7 synthetic satellite images Meteosat7 -",
        4: "SYNMSG synthetic satellite images MSG -",
        5: "MSG_TB MSG brightness temperature K",
        6: "MSG_TBC MSG clear-sky brightness temperature K",
        7: "MSG_RAD MSG radiance W sr-1 m-2",
        8: "MSG_RADC MSG clear-sky radiance W sr-1 m-2",
    },
}


def format_product_cosmo(v):
    # get product description from table
    if v["style"] == "GRIB1":
        if v["origin"] in cosmo_centres:
            table, product = v["table"], v["product"]
            if table > 127:
                try:
                    return prodtab[table][product]
                except KeyError:
                    return None
    # keep default behaviour
    return None


Formatter.register("product", format_product_cosmo)
