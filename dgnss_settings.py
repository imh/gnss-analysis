import numpy as np
import glue
import swiftnav.coord_system as cs
import copy
__author__ = 'imh'


class DgnssSettings():
    def __init__(self,
                 phase_var_test=None, code_var_test=None,
                 phase_var_kf=None, code_var_kf=None,
                 pos_trans_var=None, vel_trans_var=None, int_trans_var=None,
                 amb_drift_var=None,
                 pos_init_var=None, vel_init_var=None, amb_init_var=None,
                 new_int_var=None):
        self.phase_var_test = max(0, phase_var_test)
        self.code_var_test  = max(0, code_var_test)
        self.phase_var_kf   = max(0, phase_var_kf)
        self.code_var_kf    = max(0, code_var_kf)
        self.pos_trans_var  = max(0, pos_trans_var)
        self.vel_trans_var  = max(0, vel_trans_var)
        self.int_trans_var  = max(0, int_trans_var)
        self.amb_drift_var  = max(0, amb_drift_var)
        self.pos_init_var   = max(0, pos_init_var)
        self.vel_init_var   = max(0, vel_init_var)
        self.amb_init_var   = max(0, amb_init_var)
        self.new_int_var    = max(0, new_int_var)

    def store(self, store_attrs):
        for k, v in self.__dict__.iteritems():
            try:
                setattr(store_attrs, k, v)
            except:
                print "Unable to store '%s'" % k

    def __str__(self):
        strout = '<DgnssSettings where\n'
        for k, v in self.__dict__.iteritems():
            strout += k + '\t = \t' + str(v) + '\n'
        return strout + str('>')

class DgnssSettingsSweep():
    def __init__(self,
                 phase_var_test, code_var_test,
                 phase_var_kf, code_var_kf,
                 pos_trans_var, vel_trans_var, int_trans_var,
                 amb_drift_var,
                 pos_init_var, vel_init_var, amb_init_var,
                 new_int_var):
        self.phase_var_test = phase_var_test
        self.code_var_test  = code_var_test
        self.phase_var_kf   = phase_var_kf
        self.code_var_kf    = code_var_kf
        self.pos_trans_var  = pos_trans_var
        self.vel_trans_var  = vel_trans_var
        self.int_trans_var  = int_trans_var
        self.amb_drift_var  = amb_drift_var
        self.pos_init_var   = pos_init_var
        self.vel_init_var   = vel_init_var
        self.amb_init_var   = amb_init_var
        self.new_int_var    = new_int_var

    def sweep(self):
        dgnss_settings = DgnssSettings()
        lst = []
        self.sweep_vars(dgnss_settings, self.__dict__.items(), lst)
        return lst

    def sweep_vars(self, dgnss_settings, vars, lst):
        if len(vars) == 0:
            lst += [copy.copy(dgnss_settings)]
        else:
            var_name = vars[0][0]
            var = vars[0][1]
            for val in var.xs:
                setattr(dgnss_settings, var_name, val)
                self.sweep_vars(dgnss_settings, vars[1:], lst)


class LinSweep():
    def __init__(self, min_x, max_x, num_steps):
        k = (max_x - min_x)/(num_steps - 1.0)
        self.xs = min_x + np.array(range(num_steps)) * k

class ExpSweep():
    def __init__(self, min_x, max_x, num_steps):
        k = np.log(float(max_x) / min_x) / (num_steps - 1)
        self.xs = min_x * np.exp(np.array(range(num_steps)) * k)

class ConstSweep():
    def __init__(self, const):
        self.xs = np.array([const])


def sweep_analyze(b, ecef,
                  data_filename, data_key,
                  almanac_filename, analysis_filename):

    phase_var_test = ExpSweep(1e-6, 1, 6)
    code_var_test = ExpSweep(1, 1e5, 5)

    phase_var_kf = ExpSweep(1e-6, 1, 5)
    code_var_kf = ExpSweep(1, 1e5, 5)

    pos_trans_var = ConstSweep(1e-1)
    vel_trans_var = ConstSweep(1e-5)
    int_trans_var = ConstSweep(1e-8)
    amb_drift_var = ExpSweep(1e-12, 1e-3, 5)

    pos_init_var = ConstSweep(1e2)
    vel_init_var = ConstSweep(4e2)
    amb_init_var = ExpSweep(1e5, 1e14, 4)

    new_int_var = ExpSweep(1e4, 1e14, 5)

    settings_sweep = DgnssSettingsSweep(phase_var_test, code_var_test,
                                        phase_var_kf, code_var_kf,
                                        pos_trans_var, vel_trans_var, int_trans_var,
                                        amb_drift_var,
                                        pos_init_var, vel_init_var, amb_init_var,
                                        new_int_var)

    id = 0
    all_settings = settings_sweep.sweep()
    num_settings = len(all_settings)
    for i, settings in enumerate(all_settings):
        print 'iteration ' + str(i+1) + ' / ' + str(num_settings)
        full_analysis_filename = analysis_filename + "_" + str(id)
        print settings
        glue.analyze(b, ecef, settings,
                     data_filename, data_key,
                     almanac_filename, full_analysis_filename)
        id += 1

if __name__=="__main__":
    b = np.array([15, 0, 0])
    llh = np.array([np.deg2rad(37.7798), np.deg2rad(-122.3923), 40])
    ecef = cs.wgsllh2ecef(*llh)
    data_filename = "/home/imh/software/swift/projects/integer-ambiguity/fake.hd5"
    data_key = 'sd'
    almanac_filename = "/home/imh/software/swift/projects/integer-ambiguity/001.ALM"
    analysis_filename = "/home/imh/software/swift/analyses/fake.hd5"

    sweep_analyze(b, ecef,
                  data_filename, data_key,
                  almanac_filename, analysis_filename)